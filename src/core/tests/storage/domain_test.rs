use anyhow::Result;
use authguard_adapter_rust::{AccessGrantSet, RequestAccess};
use serde_json::{json, Value};
use sigbot_core::context::identity::RequestIdentity;
use sigbot_core::modules::domain::handler::resource::ResourceHandler;
use sigbot_core::storage::IGenericRepository;
use sigbot_core::storage::{DomainPool, DomainRepository};
use sigbot_types::domain::{Domain, ResourceCommand, ResourceOperation};
use sigbot_types::Uuid;

struct Fixture {
    pool: DomainPool,
    tenant: Uuid,
}
impl Fixture {
    fn identity(&self) -> RequestIdentity {
        let base = format!("urn:iam:default:sigbot:global:{}:", self.tenant);
        RequestIdentity::new(RequestAccess {
            principal_id: "domain-test".into(),
            action: "sigbot:write".into(),
            resource_urn: format!("{base}environment/dev/tenant/all"),
            grants: AccessGrantSet::new(vec![format!("{base}environment/dev/**")], vec![]),
        })
        .unwrap()
    }
    fn repository(&self, domain: Domain) -> DomainRepository {
        let pool = match &self.pool {
            DomainPool::SQLite(pool) => DomainPool::SQLite(pool.clone()),
            DomainPool::Postgres(pool) => DomainPool::Postgres(pool.clone()),
        };
        DomainRepository::new(pool, domain, self.tenant, "dev".into())
    }
    async fn command(
        &self,
        domain: Domain,
        operation: ResourceOperation,
        id: Option<Uuid>,
        fields: Value,
    ) -> Result<Value> {
        ResourceHandler::execute_scoped(
            self.repository(domain),
            &self.identity(),
            ResourceCommand {
                domain,
                operation,
                id,
                env: "dev".into(),
                fields: fields.as_object().unwrap().clone(),
                limit: 50,
                offset: 0,
            },
        )
        .await
    }
    async fn exercise(&self) -> Result<()> {
        let identity = self.identity();
        let verified = RequestIdentity::from_verified_value(identity.to_verified_value())?;
        verified.require_resource("dev", "strategy", "all")?;
        assert!(verified.require_resource("prod", "strategy", "all").is_err());
        self.command(
            Domain::Tenant,
            ResourceOperation::Create,
            None,
            json!({"name":"tenant"}),
        )
        .await?;
        let workflow = self
            .command(
                Domain::Workflow,
                ResourceOperation::Create,
                None,
                json!({"name":"workflow","dag_config":{"nodes":[],"connections":[]}}),
            )
            .await?;
        let workflow_id: Uuid = serde_json::from_value(workflow["id"].clone())?;
        let run = Uuid::now_v7();
        match &self.pool {
            DomainPool::SQLite(pool) => {
                sqlx::query(
                    "INSERT INTO wf_run(run_id,workflow_id,env,tenant_id,state) VALUES(?1,?2,'dev',?3,'queued')",
                )
                .bind(run.to_string())
                .bind(workflow_id.to_string())
                .bind(self.tenant.to_string())
                .execute(pool)
                .await?;
            }
            DomainPool::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO wf_run(run_id,workflow_id,env,tenant_id,state) VALUES($1,$2,'dev',$3,'queued')",
                )
                .bind(run)
                .bind(workflow_id)
                .bind(self.tenant)
                .execute(pool)
                .await?;
            }
        }
        let ledger = self
            .command(
                Domain::Ledger,
                ResourceOperation::Create,
                None,
                json!({"account_id":Uuid::now_v7(),"exchange":"binance","mode":"paper"}),
            )
            .await?;
        let ledger_id: Uuid = serde_json::from_value(ledger["id"].clone())?;
        let ledger_data = self
            .command(Domain::Ledger, ResourceOperation::Get, Some(ledger_id), json!({}))
            .await?;
        assert!(ledger_data["data"]["balances"].is_array());
        assert!(ledger_data["data"]["entries"].is_array());
        self.command(
            Domain::Tenant,
            ResourceOperation::Update,
            Some(self.tenant),
            json!({"name":"tenant with owned entities"}),
        )
        .await?;
        for domain in Domain::ALL {
            let fields = match domain {
                Domain::Tenant | Domain::Workflow | Domain::Ledger => continue,
                Domain::Cluster => {
                    json!({"name":"cluster","provider":"docker","conn_url":"unix:///var/run/docker.sock","auth_config":{"auth_type":"docker_anonymous"},"configuration":{"runtime_type":"docker","network":"isolated-test","environment_file":format!("tenant-{}-runtime",self.tenant)}})
                }
                Domain::Strategy => {
                    json!({"name":"strategy","code":"class Strategy: pass","default_parameters":{"window":5}})
                }
                Domain::Order => {
                    json!({"wallet_id":ledger_id,"producer_node_id":1,"run_id":run,"seq":0,"exchange":"binance","symbol":"BTCUSDT","intent":{"account_id":ledger_data["data"]["account_id"],"wallet_id":ledger_id,"exchange":"binance","symbol":"BTCUSDT","side":"BUY","quantity":"0.1","order_type":"MARKET","limit_price":null,"reduce_only":false,"market":{"source":"fixture","event_time":chrono::Utc::now()-chrono::Duration::seconds(2),"available_time":chrono::Utc::now()-chrono::Duration::seconds(1),"price":"100"}},"reserve_asset":"USDT","reserve_amount":"10.5","state":"pending"})
                }
                _ => {
                    json!({"name":"external","provider":"custom","transport":"http","configuration":{"endpoint":"https://example.test"},"secret_ref":"secret://test_credentials"})
                }
            };
            let created = self.command(domain, ResourceOperation::Create, None, fields).await?;
            let id: Uuid = serde_json::from_value(created["id"].clone())?;
            let data = self
                .command(domain, ResourceOperation::Get, Some(id), json!({}))
                .await?;
            assert_eq!(data["data"]["id"], json!(id));
            assert_eq!(
                self.command(domain, ResourceOperation::List, None, json!({})).await?["data"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
            let changed = if domain == Domain::Order {
                json!({"reserve_amount":"11.5"})
            } else {
                json!({"name":"updated"})
            };
            self.command(domain, ResourceOperation::Update, Some(id), changed)
                .await?;
            assert!(self
                .command(
                    domain,
                    ResourceOperation::Update,
                    Some(id),
                    json!({"unknown_field":"no"})
                )
                .await
                .is_err());
            assert!(self
                .command(
                    domain,
                    ResourceOperation::Update,
                    Some(id),
                    json!({"tenant_id":Uuid::now_v7()})
                )
                .await
                .is_err());
            let other = DomainRepository::new(
                match &self.pool {
                    DomainPool::SQLite(p) => DomainPool::SQLite(p.clone()),
                    DomainPool::Postgres(p) => DomainPool::Postgres(p.clone()),
                },
                domain,
                Uuid::now_v7(),
                "dev".into(),
            );
            assert!(other.get(id).await?.is_none());
            assert_eq!(other.delete_by_id(id).await?, 0);
            self.command(domain, ResourceOperation::Delete, Some(id), json!({}))
                .await?;
            assert!(self.repository(domain).get(id).await?.is_none());
        }
        self.command(
            Domain::Ledger,
            ResourceOperation::Update,
            Some(ledger_id),
            json!({"account_type":"spot"}),
        )
        .await?;
        self.command(Domain::Ledger, ResourceOperation::Delete, Some(ledger_id), json!({}))
            .await?;
        self.command(
            Domain::Workflow,
            ResourceOperation::Update,
            Some(workflow_id),
            json!({"name":"updated workflow"}),
        )
        .await?;
        assert!(self
            .command(
                Domain::Workflow,
                ResourceOperation::Update,
                Some(workflow_id),
                json!({"status":"RUNNING"})
            )
            .await
            .is_err());
        match &self.pool {
            DomainPool::SQLite(pool) => {
                sqlx::query("DELETE FROM wf_run").execute(pool).await?;
            }
            DomainPool::Postgres(pool) => {
                sqlx::query("DELETE FROM wf_run").execute(pool).await?;
            }
        }
        self.command(
            Domain::Workflow,
            ResourceOperation::Delete,
            Some(workflow_id),
            json!({}),
        )
        .await?;
        self.command(
            Domain::Tenant,
            ResourceOperation::Update,
            Some(self.tenant),
            json!({"name":"updated tenant"}),
        )
        .await?;
        self.command(Domain::Tenant, ResourceOperation::Delete, Some(self.tenant), json!({}))
            .await?;
        Ok(())
    }
}

#[tokio::test]
async fn sqlite_ten_domains_crud_and_isolation() -> Result<()> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    sqlx::raw_sql(include_str!("../../../../migration/20261031000000/sqlite/init.ddl.sql"))
        .execute(&pool)
        .await?;
    Fixture {
        pool: DomainPool::SQLite(pool),
        tenant: Uuid::now_v7(),
    }
    .exercise()
    .await
}

#[tokio::test]
#[ignore = "requires isolated SIGBOT_DOMAIN_TEST_POSTGRES_URL"]
async fn postgres_ten_domains_crud_and_isolation() -> Result<()> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var("SIGBOT_DOMAIN_TEST_POSTGRES_URL")?)
        .await?;
    sqlx::raw_sql(include_str!(
        "../../../../migration/20261031000000/postgres/init.ddl.sql"
    ))
    .execute(&pool)
    .await?;
    Fixture {
        pool: DomainPool::Postgres(pool),
        tenant: Uuid::now_v7(),
    }
    .exercise()
    .await
}
