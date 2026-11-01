// SPDX-License-Identifier: GPL-3.0-only

use crate::binance::binance::BinanceDatafeed;
use crate::datafeed::IDatafeed;
use crate::fed::federal_reserve::FederalReserveDatafeed;
use crate::treasury::treasury::TreasuryDatafeed;
use crate::truthsocial::truthsocial::TruthSocialDatafeed;
use crate::twitter::twitter::TwitterDatafeed;
use anyhow::Context;
use anyhow::Error;
use anyhow::Result;
use common_context::ApplicationContext;
use common_telemetry::debug;
use common_telemetry::info;
use sigbot_messager::server::MessagerServer;
use sigbot_types::datafeed::datafeed::DatafeedProvider;
use sigbot_types::datafeed::DatafeedArgument;
use sigbot_types::ArgumentConfig;
use std::sync::Arc;

pub struct SigbotDatafeedIngestor {}

impl SigbotDatafeedIngestor {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub async fn startup(matches: &clap::ArgMatches, verbose: bool) {
        common_telemetry::readiness::RuntimeReadiness::business("datafeed", false);
        debug!("Initializing Datafeed clients.");
        let (datafeeds, argument) = SigbotDatafeedIngestor::configure(matches, verbose)
            .await
            .expect("Failed to initialize Datafeed clients.");
        info!("Initialized Datafeed clients. {}", datafeeds.len());

        debug!("Initializing Messager client.");
        let messager = MessagerServer::connect(matches, argument.messager_config.clone())
            .await
            .expect("Failed to initialize Messager client.");
        info!("Initialized Messager client. {}", messager.provider().as_str());

        let properties = argument
            .datafeed_config
            .properties
            .as_ref()
            .expect("datafeed properties required");
        if properties.contains_key("snapshot_input_node_ids") {
            let key = sigbot_utils::providers::ProviderInstanceId::configured("market_query", properties)
                .expect("invalid query identity");
            let context = ApplicationContext::global();
            let properties = properties.clone();
            let messager = messager.clone();
            context
                .register_with_shutdown_phase::<crate::query::market_query::MarketQueryService, _>(
                    &key,
                    20,
                    move || {
                        let properties = properties.clone();
                        let messager = messager.clone();
                        async move {
                            let query =
                                crate::query::market_query::MarketQueryService::from_properties(&properties).await?;
                            if let Err(error) = query.start(messager).await {
                                query.close().await;
                                return Err(error);
                            }
                            Ok(query)
                        }
                    },
                )
                .expect("query registration failed");
            context
                .get::<crate::query::market_query::MarketQueryService>(&key)
                .await
                .expect("query startup failed");
        }
        for datafeed in datafeeds.iter() {
            let provider = datafeed.provider().as_str().to_lowercase();
            let key =
                sigbot_utils::providers::ProviderInstanceId::configured(&format!("ingest:{provider}"), properties)
                    .expect("invalid ingestor identity");
            let context = ApplicationContext::global();
            let properties = properties.clone();
            let messager = messager.clone();
            context
                .register_with_shutdown_phase::<crate::datafeed::PersistedDatafeedIngestor, _>(&key, 20, move || {
                    let properties = properties.clone();
                    let provider = provider.clone();
                    let messager = messager.clone();
                    async move {
                        let ingestor = Arc::new(
                            crate::datafeed::PersistedDatafeedIngestor::from_properties(&properties, &provider).await?,
                        );
                        ingestor.start_relay(messager).await?;
                        Ok(ingestor)
                    }
                })
                .expect("ingestor registration failed");
            let ingestor = context
                .get::<crate::datafeed::PersistedDatafeedIngestor>(&key)
                .await
                .expect("ingestor startup failed");
            let handler: crate::social::social::FeedHandler = Arc::new(move |data| {
                let ingestor = ingestor.clone();
                Box::pin(async move {
                    ingestor.ingest(&data).await?;
                    Ok(data)
                })
            });
            datafeed
                .subscribe(handler)
                .await
                .expect("failed to subscribe datafeed source");
        }
        info!("Subscribed to Datafeed clients.");
        common_telemetry::readiness::RuntimeReadiness::business("datafeed", true);
    }

    pub async fn shutdown() {
        common_telemetry::readiness::RuntimeReadiness::business("datafeed", false);
        ApplicationContext::global()
            .close_all::<crate::query::market_query::MarketQueryService>()
            .await
            .expect("query shutdown failed");
        ApplicationContext::global()
            .close_all::<crate::datafeed::PersistedDatafeedIngestor>()
            .await
            .expect("ingestor shutdown failed");
        info!("Shutting down Datafeed clients.");
        ApplicationContext::global()
            .close_all::<dyn crate::datafeed::IDatafeed>()
            .await
            .expect("provider shutdown failed");
        info!("Shutdown Datafeed clients.");

        info!("Shutting down Messager client.");
        info!("Shutdown Messager client.");
    }
}

impl SigbotDatafeedIngestor {
    pub async fn configure(
        matches: &clap::ArgMatches,
        _verbose: bool,
    ) -> Result<(Arc<Vec<Arc<dyn IDatafeed>>>, Arc<DatafeedArgument>), Error> {
        // e.g '--datafeed-providers=binance'
        let providers = matches
            .try_get_one::<String>("DATAFEED_PROVIDERS")
            .map(|s| {
                s.map(|s| s.to_owned())
                    .unwrap_or_else(|| DatafeedProvider::Binance.as_str().to_owned())
            })
            .expect("Failed to parse the datafeed providers from the command line arguments.")
            .to_uppercase();

        debug!("Registering Datafeed: {}", &providers);

        // e.g '--datafeed-configuration=<base64_encoded_json_string>'
        let configuration = matches
            .try_get_one::<String>("DATAFEED_CONFIGURATION")
            .map(|s| s.map(|s| s.to_owned()).unwrap_or_default())
            .expect("Failed to parse the configuration from the command line arguments.");

        let argument = Arc::new(
            DatafeedArgument::from_json(
                &ArgumentConfig::decode(&configuration).context("Failed to decode datafeed configuration")?,
            )
            .context("Failed to parse datafeed configuration")?,
        );

        let properties = argument
            .datafeed_config
            .properties
            .as_ref()
            .context("datafeed properties required")?;
        let query_only = properties
            .get("query_only")
            .map(|value| value.parse::<bool>())
            .transpose()
            .context("query_only must be true or false")?
            .unwrap_or(false);
        if query_only {
            anyhow::ensure!(
                properties.contains_key("snapshot_input_node_ids"),
                "query_only requires declared query consumers"
            );
            return Ok((Arc::new(Vec::new()), argument));
        }

        let mut datafeeds = Vec::new();
        for provider in providers
            .split(',')
            .map(DatafeedProvider::of)
            .collect::<Result<Vec<_>, _>>()?
        {
            let instance_id = sigbot_utils::providers::ProviderInstanceId::configured(provider.as_str(), &*argument)?;
            let configured_argument = argument.clone();
            let context = ApplicationContext::global();
            context.register_if_absent::<dyn IDatafeed, _>(&instance_id, move || {
                let argument = configured_argument.clone();
                let provider = provider.clone();
                async move {
                    let instance: Arc<dyn IDatafeed> = match provider {
                        DatafeedProvider::Binance => BinanceDatafeed::new().await,
                        DatafeedProvider::Twitter => TwitterDatafeed::new().await,
                        DatafeedProvider::TruthSocial => TruthSocialDatafeed::new().await,
                        DatafeedProvider::Fed => FederalReserveDatafeed::new(),
                        DatafeedProvider::Treasury => TreasuryDatafeed::new(),
                    };
                    instance.configure(argument).await?;
                    Ok(instance)
                }
            })?;
            datafeeds.push(context.get::<dyn IDatafeed>(&instance_id).await?);
        }
        Ok((Arc::new(datafeeds), argument))
    }

    pub async fn close(name: String) -> anyhow::Result<()> {
        ApplicationContext::global().close_for::<dyn IDatafeed>(&name).await
    }
}
