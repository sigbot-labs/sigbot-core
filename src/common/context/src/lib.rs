// SPDX-License-Identifier: GPL-3.0-or-later

//! Typed registration and lazy lifecycle management for application providers.
//!
//! Domain interfaces inherit [`IProvider`]. Register constructors that capture
//! their configuration and dependencies, then retrieve instances by the domain
//! interface type and instance ID. Dependencies must be acyclic; assign
//! dependents higher shutdown phases so they close before their dependencies.
//! Register another interface of the same object with
//! [`ApplicationContext::register_with`] to share one initialization and close.
//!
//! ```
//! use std::{any::Any, sync::Arc};
//! use anyhow::Result;
//! use async_trait::async_trait;
//! use common_context::{ApplicationContext, IProvider};
//!
//! trait ITransport: IProvider {
//!     fn name(&self) -> &str;
//! }
//!
//! trait IMonitor: IProvider {
//!     fn healthy(&self) -> bool;
//! }
//!
//! struct LocalTransport;
//!
//! #[async_trait]
//! impl IProvider for LocalTransport {
//!     async fn close(&self) -> Result<()> { Ok(()) }
//! }
//!
//! impl ITransport for LocalTransport {
//!     fn name(&self) -> &str { "local" }
//! }
//!
//! impl IMonitor for LocalTransport {
//!     fn healthy(&self) -> bool { true }
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let context = ApplicationContext::new();
//! context.register::<dyn ITransport, _>("local-main", || async {
//!     Ok(Arc::new(LocalTransport) as Arc<dyn ITransport>)
//! })?;
//! context.register_with::<dyn IMonitor, dyn ITransport>("local-main", |transport| {
//!     let object: Arc<dyn Any + Send + Sync> = transport;
//!     let local = object.downcast::<LocalTransport>()
//!         .map_err(|_| anyhow::anyhow!("unexpected transport implementation"))?;
//!     Ok(local as Arc<dyn IMonitor>)
//! })?;
//! context.init().await?;
//! let transport = context.get::<dyn ITransport>("local-main").await?;
//! assert_eq!(transport.name(), "local");
//! assert!(context.get::<dyn IMonitor>("local-main").await?.healthy());
//! context.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! Types without the top-level lifecycle interface cannot be registered:
//!
//! ```compile_fail
//! use std::sync::Arc;
//! use common_context::ApplicationContext;
//!
//! struct Unmanaged;
//!
//! ApplicationContext::new().register::<Unmanaged, _>("default", || async {
//!     Ok(Arc::new(Unmanaged))
//! }).unwrap();
//! ```

/// Shared provider contracts and application lifecycle management.
pub mod lifecycle {
    pub mod application;
    pub mod provider;
}

pub use lifecycle::{application::ApplicationContext, provider::IProvider};
