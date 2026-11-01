// SPDX-License-Identifier: GPL-3.0-only

use axum::Router;
use common_context::IProvider;

pub mod workflow;

pub trait IA2aApi: IProvider {
    fn router(&self) -> Router;
}
