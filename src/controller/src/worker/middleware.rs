//! Tenant middleware topology and component configuration binding.
use crate::worker::plan::WorkerPlan;
use anyhow::Context;
use anyhow::Result;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde_json::json;
use serde_json::Value;
use sigbot_types::controller::{MiddlewareDeployment, WorkerInfo, WorkerProvider};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MiddlewareKind {
    Redis,
    Pgvector,
    Timescaledb,
    Emqx,
}

impl MiddlewareKind {
    pub const ALL: [Self; 4] = [Self::Redis, Self::Pgvector, Self::Timescaledb, Self::Emqx];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Redis => "redis",
            Self::Pgvector => "pgvector",
            Self::Timescaledb => "timescaledb",
            Self::Emqx => "emqx",
        }
    }

    pub const fn port(self) -> u16 {
        match self {
            Self::Redis => 6379,
            Self::Pgvector | Self::Timescaledb => 5432,
            Self::Emqx => 1883,
        }
    }
}

pub struct MiddlewareTopology<'a> {
    target: &'a WorkerInfo,
}

impl<'a> MiddlewareTopology<'a> {
    pub const fn new(target: &'a WorkerInfo) -> Self {
        Self { target }
    }

    pub fn deployment(&self, kind: MiddlewareKind) -> Option<&'a MiddlewareDeployment> {
        match kind {
            MiddlewareKind::Redis => self.target.middleware.redis.as_ref(),
            MiddlewareKind::Pgvector => self.target.middleware.pgvector.as_ref(),
            MiddlewareKind::Timescaledb => self.target.middleware.timescaledb.as_ref(),
            MiddlewareKind::Emqx => self.target.middleware.emqx.as_ref(),
        }
    }

    pub fn name(&self, kind: MiddlewareKind) -> String {
        let identity = self.target.id.simple().to_string();
        format!("sigbot-{}-{}", &identity[..12], kind.as_str())
    }

    pub fn host(&self, kind: MiddlewareKind) -> String {
        let name = self.name(kind);
        if kind == MiddlewareKind::Redis && self.target.provider == WorkerProvider::Docker {
            format!("{name}-0")
        } else {
            name
        }
    }

    pub fn bind(&self, plan: &mut WorkerPlan) -> Result<()> {
        if let Some(timescaledb) = self.deployment(MiddlewareKind::Timescaledb) {
            let _ = timescaledb;
            plan.environment
                .insert("SIGBOT__STORAGE__PROVIDER".into(), "Postgres".into());
            plan.environment.insert(
                "SIGBOT__STORAGE__POSTGRES__HOST".into(),
                self.host(MiddlewareKind::Timescaledb),
            );
            plan.environment
                .insert("SIGBOT__STORAGE__POSTGRES__PORT".into(), "5432".into());
            plan.environment
                .insert("SIGBOT__STORAGE__POSTGRES__USERNAME".into(), "postgres".into());
            plan.environment
                .insert("SIGBOT__STORAGE__POSTGRES__DATABASE".into(), "postgres".into());
            plan.environment
                .insert("SIGBOT__STORAGE__POSTGRES__SCHEMA".into(), "public".into());
        }
        if self.deployment(MiddlewareKind::Pgvector).is_some() {
            plan.environment.insert(
                "SIGBOT__LLM__VECDB__PG_VECTOR__HOST".into(),
                self.host(MiddlewareKind::Pgvector),
            );
            plan.environment
                .insert("SIGBOT__LLM__VECDB__PG_VECTOR__PORT".into(), "5432".into());
            plan.environment
                .insert("SIGBOT__LLM__VECDB__PG_VECTOR__USERNAME".into(), "postgres".into());
            plan.environment
                .insert("SIGBOT__LLM__VECDB__PG_VECTOR__DATABASE".into(), "postgres".into());
            plan.environment
                .insert("SIGBOT__LLM__VECDB__PG_VECTOR__SCHEMA".into(), "public".into());
        }
        if self.deployment(MiddlewareKind::Redis).is_some() {
            plan.environment
                .insert("SIGBOT__CACHE__PROVIDER".into(), "Redis".into());
            plan.environment.insert(
                "SIGBOT__CACHE__REDIS__NODES".into(),
                format!("redis://{}:6379", self.host(MiddlewareKind::Redis)),
            );
        }
        if self.deployment(MiddlewareKind::Emqx).is_some() {
            let encoded = plan
                .command
                .get_mut(5)
                .context("worker configuration argument missing")?;
            let mut argument: Value = serde_json::from_slice(&STANDARD.decode(encoded.as_bytes())?)?;
            let properties = argument
                .get_mut("messager_config")
                .and_then(Value::as_object_mut)
                .context("messager configuration required")?
                .entry("properties")
                .or_insert_with(|| json!({}))
                .as_object_mut()
                .context("messager properties must be an object")?;
            properties.insert("mqtt_server".into(), json!(self.host(MiddlewareKind::Emqx)));
            properties.insert("mqtt_port".into(), json!(MiddlewareKind::Emqx.port().to_string()));
            *encoded = STANDARD.encode(serde_json::to_vec(&argument)?);
        }
        Ok(())
    }
}
