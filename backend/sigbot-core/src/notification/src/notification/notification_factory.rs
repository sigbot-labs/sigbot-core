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
//
// IMPORTANT: Any software that fully or partially contains or uses materials
// covered by this license must also be released under the GNU GPL license.
// This includes modifications and derived works.

use anyhow::Error;
use async_trait::async_trait;
use common_telemetry::{debug, info};
use lazy_static::lazy_static;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[async_trait]
pub trait ISigbotNotificationOperation: Send + Sync {
    async fn init(&self);
    async fn close(&self);
    async fn send_message(&self, to: &str, message: &str) -> Result<String, Error>;
}

lazy_static! {
    static ref SINGLE_INSTANCE: RwLock<SigbotNotificationFactory> = RwLock::new(SigbotNotificationFactory::new());
}

pub struct SigbotNotificationFactory {
    pub implementations: HashMap<String, Arc<dyn ISigbotNotificationOperation + Send + Sync>>,
}

impl SigbotNotificationFactory {
    fn new() -> Self {
        SigbotNotificationFactory {
            implementations: HashMap::new(),
        }
    }

    pub fn get() -> &'static RwLock<SigbotNotificationFactory> {
        &SINGLE_INSTANCE
    }

    pub async fn init() {
        info!("Register to All Sigbot notification operations ...");
        unimplemented!()
    }

    fn register<T: ISigbotNotificationOperation + Send + Sync + 'static>(
        &mut self,
        name: String,
        handler: Arc<T>,
    ) -> Result<Arc<T>, Error> {
        if self.implementations.contains_key(&name) {
            debug!("Already register the sigbot notification operation '{}'", name);
            return Ok(handler);
        }
        self.implementations.insert(name, handler.to_owned());
        Ok(handler)
    }

    pub async fn get_implementation(
        name: String,
    ) -> Result<Arc<dyn ISigbotNotificationOperation + Send + Sync>, Error> {
        // If the read lock is poisoned, the program will panic.
        let this = SigbotNotificationFactory::get().read().unwrap();
        if let Some(implementation) = this.implementations.get(&name) {
            Ok(implementation.to_owned())
        } else {
            let errmsg = format!("Could not obtain registered sigbot notification operation '{}'.", name);
            return Err(Error::msg(errmsg));
        }
    }

    pub async fn close() {
        let this = SigbotNotificationFactory::get().read().unwrap();
        for implementation in this.implementations.values() {
            implementation.close().await;
        }
    }
}

#[cfg(test)]
mod tests {}
