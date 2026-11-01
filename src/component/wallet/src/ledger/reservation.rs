// SPDX-License-Identifier: GPL-3.0-only

use anyhow::Result;
use sigbot_core::modules::wallet::handler::wallet::WalletReservationHandler;
use sigbot_messager::messager::ReplyPairingCache;
use sigbot_types::messager::envelope::MessageEnvelope;
use sigbot_types::wallet::reservation::ReservationRequest;

pub struct WalletReservationProcessor {
    handler: WalletReservationHandler,
}

impl WalletReservationProcessor {
    pub fn new(handler: WalletReservationHandler) -> Self {
        Self { handler }
    }

    pub async fn process(&self, message: &MessageEnvelope<ReservationRequest>) -> Result<bool> {
        let result = self.handler.process(message).await?;
        if let (Some(pairing), Some(response)) = (message.request_span_id.as_deref(), result.response.as_ref()) {
            ReplyPairingCache::shared().remember(response, pairing)?;
        }
        Ok(result.processed)
    }
}
