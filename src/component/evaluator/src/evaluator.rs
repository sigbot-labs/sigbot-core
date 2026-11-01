// SPDX-License-Identifier: GPL-3.0-only

use async_trait::async_trait;
use common_context::IProvider;
use sigbot_messager::messager::IMessager;
use sigbot_types::evaluator::evaluator::EvaluatorProvider;
use sigbot_types::evaluator::EvaluatorArgument;
use std::sync::Arc;

/// Core trait for evaluation executors
/// Includes both core functionality and lifecycle management
#[async_trait]
pub trait IEvaluator: IProvider {
    fn provider(&self) -> EvaluatorProvider;

    /// This will be called by WorkflowManager when the workflow node starts
    async fn startup(
        self: Arc<Self>,
        argument: Arc<EvaluatorArgument>,
        messager: Arc<dyn IMessager>,
    ) -> anyhow::Result<()>;
}
