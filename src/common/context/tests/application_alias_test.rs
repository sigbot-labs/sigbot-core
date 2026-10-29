use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use common_context::{ApplicationContext, IProvider};
use std::{
    any::Any,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{sync::Notify, time::timeout};

const DEADLINE: Duration = Duration::from_secs(3);

trait PrimaryDomain: IProvider {}
trait SecondaryDomain: IProvider {}
trait ThirdDomain: IProvider {}

#[derive(Default)]
struct Probe {
    inits: AtomicUsize,
    closes: AtomicUsize,
    init_fail_once: AtomicBool,
    init_block_once: AtomicBool,
    close_fail_once: AtomicBool,
    close_block_once: AtomicBool,
    entered: Notify,
    label: &'static str,
    order: Option<Arc<Mutex<Vec<&'static str>>>>,
}

impl PrimaryDomain for Probe {}
impl SecondaryDomain for Probe {}
impl ThirdDomain for Probe {}

impl Probe {
    fn register(self: &Arc<Self>, ctx: &ApplicationContext, instance: &str, builds: &Arc<AtomicUsize>) -> Result<()> {
        let (value, calls) = (Arc::clone(self), Arc::clone(builds));
        ctx.register::<dyn PrimaryDomain, _>(instance, move || {
            let (value, calls) = (Arc::clone(&value), Arc::clone(&calls));
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                Ok(value as Arc<dyn PrimaryDomain>)
            }
        })
    }

    fn secondary(source: Arc<dyn PrimaryDomain>) -> Result<Arc<dyn SecondaryDomain>> {
        let source: Arc<dyn Any + Send + Sync> = source;
        let value = source
            .downcast::<Self>()
            .map_err(|_| anyhow!("unexpected source type"))?;
        Ok(value)
    }

    fn third(source: Arc<dyn PrimaryDomain>) -> Result<Arc<dyn ThirdDomain>> {
        let source: Arc<dyn Any + Send + Sync> = source;
        let value = source
            .downcast::<Self>()
            .map_err(|_| anyhow!("unexpected source type"))?;
        Ok(value)
    }
}

#[async_trait]
impl IProvider for Probe {
    async fn init(&self) -> Result<()> {
        self.inits.fetch_add(1, Ordering::SeqCst);
        if self.init_fail_once.swap(false, Ordering::SeqCst) {
            bail!("init failed");
        }
        if self.init_block_once.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            std::future::pending::<()>().await;
        }
        tokio::task::yield_now().await;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        if self.close_fail_once.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            bail!("close failed");
        }
        if self.close_block_once.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            std::future::pending::<()>().await;
        }
        if let Some(order) = &self.order {
            order.lock().unwrap().push(self.label);
        }
        Ok(())
    }
}

#[tokio::test]
async fn aliases_share_lazy_construction_initialization_and_shutdown() {
    timeout(DEADLINE, async {
        let ctx = Arc::new(ApplicationContext::new());
        let value = Arc::new(Probe::default());
        let builds = Arc::new(AtomicUsize::new(0));
        value.register(&ctx, "shared", &builds).unwrap();
        ctx.register_with::<dyn SecondaryDomain, dyn PrimaryDomain>("shared", Probe::secondary)
            .unwrap();
        assert_eq!(builds.load(Ordering::SeqCst), 0);
        assert_eq!(value.inits.load(Ordering::SeqCst), 0);
        assert!(ctx.get::<dyn SecondaryDomain>("shared").await.is_err());
        ctx.init().await.unwrap();

        let mut jobs = Vec::new();
        for index in 0..16 {
            let ctx = Arc::clone(&ctx);
            jobs.push(tokio::spawn(async move {
                if index % 2 == 0 {
                    ctx.get::<dyn PrimaryDomain>("shared").await.unwrap() as Arc<dyn IProvider>
                } else {
                    ctx.get::<dyn SecondaryDomain>("shared").await.unwrap() as Arc<dyn IProvider>
                }
            }));
        }
        for job in jobs {
            let alias = job.await.unwrap();
            assert_eq!(Arc::as_ptr(&alias) as *const (), Arc::as_ptr(&value) as *const ());
        }
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert_eq!(value.inits.load(Ordering::SeqCst), 1);
        ctx.close().await.unwrap();
        ctx.shutdown().await.unwrap();
        assert_eq!(value.closes.load(Ordering::SeqCst), 1);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn closing_any_view_invalidates_all_views_without_rebinding_old_aliases() {
    timeout(DEADLINE, async {
        for close_alias in [true, false] {
            let ctx = ApplicationContext::new();
            let original = Arc::new(Probe::default());
            let builds = Arc::new(AtomicUsize::new(0));
            original.register(&ctx, "shared", &builds).unwrap();
            ctx.register_with::<dyn SecondaryDomain, dyn PrimaryDomain>("shared", Probe::secondary)
                .unwrap();
            ctx.register_with::<dyn ThirdDomain, dyn PrimaryDomain>("shared", Probe::third)
                .unwrap();
            ctx.init().await.unwrap();
            ctx.get::<dyn SecondaryDomain>("shared").await.unwrap();
            ctx.get::<dyn ThirdDomain>("shared").await.unwrap();
            if close_alias {
                ctx.close_for::<dyn SecondaryDomain>("shared").await.unwrap();
            } else {
                ctx.close_for::<dyn PrimaryDomain>("shared").await.unwrap();
            }
            assert!(ctx.get::<dyn PrimaryDomain>("shared").await.is_err());
            assert!(ctx.get::<dyn SecondaryDomain>("shared").await.is_err());
            assert!(ctx.get::<dyn ThirdDomain>("shared").await.is_err());
            assert_eq!(original.inits.load(Ordering::SeqCst), 1);
            assert_eq!(original.closes.load(Ordering::SeqCst), 1);
            ctx.close_for::<dyn PrimaryDomain>("shared").await.unwrap();

            let replacement = Arc::new(Probe::default());
            replacement.register(&ctx, "shared", &builds).unwrap();
            let current = ctx.get::<dyn PrimaryDomain>("shared").await.unwrap();
            assert_eq!(
                Arc::as_ptr(&current) as *const (),
                Arc::as_ptr(&replacement) as *const ()
            );
            assert!(ctx.get::<dyn ThirdDomain>("shared").await.is_err());
            assert_eq!(builds.load(Ordering::SeqCst), 2);
            ctx.close().await.unwrap();
            assert_eq!(original.closes.load(Ordering::SeqCst), 1);
            assert_eq!(replacement.closes.load(Ordering::SeqCst), 1);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn alias_registration_validates_source_duplicates_and_cycles_without_building() {
    timeout(DEADLINE, async {
        let ctx = ApplicationContext::new();
        for instance in ["missing", "", " \t"] {
            assert!(ctx
                .register_with::<dyn SecondaryDomain, dyn PrimaryDomain>(instance, Probe::secondary)
                .is_err());
        }
        let value = Arc::new(Probe::default());
        let builds = Arc::new(AtomicUsize::new(0));
        value.register(&ctx, "shared", &builds).unwrap();
        ctx.register_with::<dyn SecondaryDomain, dyn PrimaryDomain>("shared", Probe::secondary)
            .unwrap();
        assert!(ctx
            .register_with::<dyn SecondaryDomain, dyn PrimaryDomain>("shared", Probe::secondary)
            .is_err());
        assert!(ctx
            .register_with::<dyn PrimaryDomain, dyn PrimaryDomain>("shared", Ok)
            .is_err());
        assert!(ctx
            .register_with::<dyn PrimaryDomain, dyn SecondaryDomain>("shared", |_| unreachable!())
            .is_err());
        ctx.init().await.unwrap();
        ctx.register_with::<dyn ThirdDomain, dyn PrimaryDomain>("shared", Probe::third)
            .unwrap();
        ctx.close().await.unwrap();
        assert!(ctx
            .register_with::<dyn SecondaryDomain, dyn PrimaryDomain>("shared", Probe::secondary)
            .is_err());
        assert_eq!(builds.load(Ordering::SeqCst), 0);
        assert_eq!(value.inits.load(Ordering::SeqCst), 0);
        assert_eq!(value.closes.load(Ordering::SeqCst), 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn initialization_failure_through_alias_is_cleaned_and_retried_by_owner() {
    timeout(DEADLINE, async {
        let ctx = ApplicationContext::new();
        let value = Arc::new(Probe::default());
        let builds = Arc::new(AtomicUsize::new(0));
        value.init_fail_once.store(true, Ordering::SeqCst);
        value.register(&ctx, "shared", &builds).unwrap();
        ctx.register_with::<dyn SecondaryDomain, dyn PrimaryDomain>("shared", Probe::secondary)
            .unwrap();
        ctx.init().await.unwrap();
        assert!(ctx.get::<dyn SecondaryDomain>("shared").await.is_err());
        assert_eq!(value.inits.load(Ordering::SeqCst), 1);
        assert_eq!(value.closes.load(Ordering::SeqCst), 1);
        ctx.get::<dyn PrimaryDomain>("shared").await.unwrap();
        let alias = ctx.get::<dyn SecondaryDomain>("shared").await.unwrap();
        assert_eq!(Arc::as_ptr(&alias) as *const (), Arc::as_ptr(&value) as *const ());
        assert_eq!(builds.load(Ordering::SeqCst), 2);
        assert_eq!(value.inits.load(Ordering::SeqCst), 2);
        ctx.close().await.unwrap();
        assert_eq!(value.closes.load(Ordering::SeqCst), 2);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelled_alias_initialization_is_cleaned_before_either_view_retries() {
    timeout(DEADLINE, async {
        for retry_alias in [true, false] {
            let ctx = Arc::new(ApplicationContext::new());
            let value = Arc::new(Probe::default());
            let builds = Arc::new(AtomicUsize::new(0));
            value.init_block_once.store(true, Ordering::SeqCst);
            value.register(&ctx, "shared", &builds).unwrap();
            ctx.register_with::<dyn SecondaryDomain, dyn PrimaryDomain>("shared", Probe::secondary)
                .unwrap();
            ctx.init().await.unwrap();
            let cloned = Arc::clone(&ctx);
            let job = tokio::spawn(async move { cloned.get::<dyn SecondaryDomain>("shared").await });
            value.entered.notified().await;
            job.abort();
            assert!(matches!(job.await, Err(error) if error.is_cancelled()));
            assert_eq!(builds.load(Ordering::SeqCst), 1);
            assert_eq!(value.inits.load(Ordering::SeqCst), 1);
            assert_eq!(value.closes.load(Ordering::SeqCst), 0);
            if retry_alias {
                ctx.get::<dyn SecondaryDomain>("shared").await.unwrap();
            } else {
                ctx.get::<dyn PrimaryDomain>("shared").await.unwrap();
            }
            let owner = ctx.get::<dyn PrimaryDomain>("shared").await.unwrap();
            let alias = ctx.get::<dyn SecondaryDomain>("shared").await.unwrap();
            assert_eq!(Arc::as_ptr(&owner) as *const (), Arc::as_ptr(&alias) as *const ());
            assert_eq!(builds.load(Ordering::SeqCst), 2);
            assert_eq!(value.inits.load(Ordering::SeqCst), 2);
            assert_eq!(value.closes.load(Ordering::SeqCst), 1);
            ctx.close().await.unwrap();
            assert_eq!(value.closes.load(Ordering::SeqCst), 2);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn failed_or_cancelled_alias_close_retains_shared_owner_for_retry() {
    timeout(DEADLINE, async {
        for cancel in [false, true] {
            let ctx = Arc::new(ApplicationContext::new());
            let value = Arc::new(Probe::default());
            let builds = Arc::new(AtomicUsize::new(0));
            value.close_fail_once.store(!cancel, Ordering::SeqCst);
            value.close_block_once.store(cancel, Ordering::SeqCst);
            value.register(&ctx, "shared", &builds).unwrap();
            ctx.register_with::<dyn SecondaryDomain, dyn PrimaryDomain>("shared", Probe::secondary)
                .unwrap();
            ctx.register_with::<dyn ThirdDomain, dyn PrimaryDomain>("shared", Probe::third)
                .unwrap();
            ctx.init().await.unwrap();
            ctx.get::<dyn SecondaryDomain>("shared").await.unwrap();
            let cloned = Arc::clone(&ctx);
            let job = tokio::spawn(async move { cloned.close_for::<dyn SecondaryDomain>("shared").await });
            value.entered.notified().await;
            if cancel {
                job.abort();
                assert!(matches!(job.await, Err(error) if error.is_cancelled()));
            } else {
                assert!(job.await.unwrap().is_err());
            }
            assert!(ctx.get::<dyn PrimaryDomain>("shared").await.is_err());
            assert!(ctx.get::<dyn SecondaryDomain>("shared").await.is_err());
            assert!(ctx.get::<dyn ThirdDomain>("shared").await.is_err());
            assert_eq!(value.closes.load(Ordering::SeqCst), 1);
            ctx.close_for::<dyn SecondaryDomain>("shared").await.unwrap();
            assert_eq!(value.closes.load(Ordering::SeqCst), 2);
            ctx.close().await.unwrap();
            assert_eq!(value.closes.load(Ordering::SeqCst), 2);
            assert_eq!(builds.load(Ordering::SeqCst), 1);
            assert_eq!(value.inits.load(Ordering::SeqCst), 1);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn projection_failure_does_not_close_or_reinitialize_the_owner() {
    timeout(DEADLINE, async {
        let ctx = ApplicationContext::new();
        let value = Arc::new(Probe::default());
        let builds = Arc::new(AtomicUsize::new(0));
        value.register(&ctx, "shared", &builds).unwrap();
        let fail_once = AtomicBool::new(true);
        ctx.register_with::<dyn SecondaryDomain, dyn PrimaryDomain>("shared", move |source| {
            if fail_once.swap(false, Ordering::SeqCst) {
                bail!("projection failed");
            }
            Probe::secondary(source)
        })
        .unwrap();
        ctx.init().await.unwrap();
        let error = ctx.get::<dyn SecondaryDomain>("shared").await.err().unwrap();
        assert!(format!("{error:#}").contains("projection failed"));
        ctx.get::<dyn PrimaryDomain>("shared").await.unwrap();
        ctx.get::<dyn SecondaryDomain>("shared").await.unwrap();
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert_eq!(value.inits.load(Ordering::SeqCst), 1);
        assert_eq!(value.closes.load(Ordering::SeqCst), 0);
        ctx.close().await.unwrap();
        assert_eq!(value.closes.load(Ordering::SeqCst), 1);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn projecting_a_different_object_is_rejected_without_affecting_the_owner() {
    timeout(DEADLINE, async {
        let ctx = ApplicationContext::new();
        let value = Arc::new(Probe::default());
        let different = Arc::new(Probe::default());
        let builds = Arc::new(AtomicUsize::new(0));
        value.register(&ctx, "shared", &builds).unwrap();
        let unrelated = Arc::clone(&different);
        ctx.register_with::<dyn SecondaryDomain, dyn PrimaryDomain>("shared", move |_| {
            Ok(Arc::clone(&unrelated) as Arc<dyn SecondaryDomain>)
        })
        .unwrap();
        ctx.init().await.unwrap();
        assert!(ctx.get::<dyn SecondaryDomain>("shared").await.is_err());
        let owner = ctx.get::<dyn PrimaryDomain>("shared").await.unwrap();
        assert_eq!(Arc::as_ptr(&owner) as *const (), Arc::as_ptr(&value) as *const ());
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert_eq!(value.inits.load(Ordering::SeqCst), 1);
        assert_eq!(value.closes.load(Ordering::SeqCst), 0);
        ctx.close().await.unwrap();
        assert_eq!(value.closes.load(Ordering::SeqCst), 1);
        assert_eq!(different.inits.load(Ordering::SeqCst), 0);
        assert_eq!(different.closes.load(Ordering::SeqCst), 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn alias_inherits_the_owners_shutdown_phase() {
    timeout(DEADLINE, async {
        let ctx = ApplicationContext::new();
        let order = Arc::new(Mutex::new(Vec::new()));
        let shared = Arc::new(Probe {
            label: "shared",
            order: Some(Arc::clone(&order)),
            ..Probe::default()
        });
        let dependent = Arc::new(Probe {
            label: "dependent",
            order: Some(Arc::clone(&order)),
            ..Probe::default()
        });
        for (instance, phase, value) in [("shared", -10, Arc::clone(&shared)), ("dependent", -5, dependent)] {
            ctx.register_with_shutdown_phase::<dyn PrimaryDomain, _>(instance, phase, move || {
                let value = Arc::clone(&value);
                async move { Ok(value as Arc<dyn PrimaryDomain>) }
            })
            .unwrap();
        }
        ctx.register_with::<dyn SecondaryDomain, dyn PrimaryDomain>("shared", Probe::secondary)
            .unwrap();
        ctx.init().await.unwrap();
        ctx.get::<dyn SecondaryDomain>("shared").await.unwrap();
        ctx.get::<dyn PrimaryDomain>("dependent").await.unwrap();
        ctx.close().await.unwrap();
        assert_eq!(*order.lock().unwrap(), ["dependent", "shared"]);
        assert_eq!(shared.closes.load(Ordering::SeqCst), 1);
    })
    .await
    .unwrap();
}
