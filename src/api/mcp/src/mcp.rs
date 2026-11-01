// SPDX-License-Identifier: GPL-3.0-only

use common_context::IProvider;

pub mod authguard;
pub mod http;
pub mod workflow;

pub trait IMcpApi: IProvider {}
