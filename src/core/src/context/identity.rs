//! Explicit trusted identity passed from HTTP middleware to business handlers.
use anyhow::{ensure, Result};
use authguard_adapter_rust::{RequestAccess, ResourceUrn, UrnPattern};
use std::str::FromStr;

#[derive(Clone, Debug)]
pub struct RequestIdentity {
    pub access: RequestAccess,
    pub resource: ResourceUrn,
}

impl RequestIdentity {
    pub fn new(access: RequestAccess) -> Result<Self> {
        ensure!(!access.principal_id.is_empty(), "missing principal");
        let resource = ResourceUrn::from_str(&access.resource_urn)?;
        let denied = access
            .grants
            .deny_resource_urns
            .iter()
            .map(|pattern| UrnPattern::from_str(pattern))
            .collect::<Result<Vec<_>, _>>()?;
        let allowed = access
            .grants
            .allow_resource_urns
            .iter()
            .map(|pattern| UrnPattern::from_str(pattern))
            .collect::<Result<Vec<_>, _>>()?;
        ensure!(
            !denied.iter().any(|pattern| pattern.matches(&resource)),
            "resource denied"
        );
        ensure!(
            allowed.iter().any(|pattern| pattern.matches(&resource)),
            "resource not granted"
        );
        Ok(Self { access, resource })
    }

    pub fn principal_id(&self) -> &str {
        &self.access.principal_id
    }
    pub fn tenant_id(&self) -> &str {
        &self.resource.tenant
    }

    pub fn require_tenant(&self, tenant_id: i64) -> Result<()> {
        ensure!(self.tenant_id() == tenant_id.to_string(), "tenant binding mismatch");
        Ok(())
    }
    pub fn require_workflow(&self, env: &str, space_id: i64, workflow_id: Option<i64>) -> Result<()> {
        self.require_resource(
            env,
            space_id,
            "workflow",
            &workflow_id.map(|id| id.to_string()).unwrap_or_else(|| "new".into()),
        )
    }

    pub fn require_resource(&self, env: &str, space_id: i64, domain: &str, id: &str) -> Result<()> {
        ensure!(
            matches!(env, "live" | "paper" | "simulated" | "backtest") && space_id > 0,
            "invalid resource scope"
        );
        let mut resource = self.resource.clone();
        resource.path = vec![
            "environment".into(),
            env.into(),
            "space".into(),
            space_id.to_string(),
            domain.into(),
            id.into(),
        ];
        let allowed = self
            .access
            .grants
            .allow_resource_urns
            .iter()
            .map(|value| UrnPattern::from_str(value))
            .collect::<Result<Vec<_>, _>>()?;
        let denied = self
            .access
            .grants
            .deny_resource_urns
            .iter()
            .map(|value| UrnPattern::from_str(value))
            .collect::<Result<Vec<_>, _>>()?;
        ensure!(
            allowed.iter().any(|pattern| pattern.matches(&resource))
                && !denied.iter().any(|pattern| pattern.matches(&resource)),
            "resource scope denied"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct ScopeQuery {
    pub env: String,
    pub space_id: i64,
}

impl ScopeQuery {
    pub fn authorize(&self, identity: &RequestIdentity, domain: &str) -> Result<i64> {
        identity.require_resource(&self.env, self.space_id, domain, "all")?;
        Ok(identity.tenant_id().parse()?)
    }
}
