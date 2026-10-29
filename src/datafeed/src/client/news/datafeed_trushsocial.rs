// SPDX-License-Identifier: GPL-3.0-only
//! Compatibility import for the historical misspelling. New code uses `datafeed_truthsocial`.
#[deprecated(note = "use datafeed_truthsocial::SigbotTruthSocialDatafeedClient")]
pub type SigbotTrushSocialDatafeedClient = super::datafeed_truthsocial::SigbotTruthSocialDatafeedClient;
