// SPDX-License-Identifier: GNU GENERAL PUBLIC LICENSE Version 3
//
// Copyleft (c) 2024 James Wong. This file is part of James Wong.
// is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// James Wong is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with James Wong.  If not, see <https://www.gnu.org/licenses/>.

//! E2E Scenario Tests
//!
//! Scenario tests for special cases: error handling, concurrency, idempotency, etc.
//!
//! # Naming Convention
//!
//! `sNN_<category>_<action>.rs`
//! - `NN`: Stage number (10=Error, 20=Concurrent, 30=Idempotency, 40=Middleware)
//! - `<category>`: Test category
//! - `<action>`: Specific test focus
//!
//! # Stage Numbering
//!
//! | Range | Category | Description |
//! |-------|----------|-------------|
//! | 10-19 | Error | Error handling & retry |
//! | 20-29 | Concurrent | High volume & concurrency |
//! | 30-39 | Idempotency | Duplicate detection & sequencing |
//! | 40-49 | Middleware | Direct middleware verification |

pub mod s10_error_handling;         // Error handling & retry mechanism
pub mod s20_concurrent_signals;     // High volume concurrent signal processing
pub mod s30_message_idempotency;    // Message deduplication & sequencing
pub mod s40_middleware_integration; // Direct middleware integration tests
