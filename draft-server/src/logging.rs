use std::env::var;
use tracing_honeycomb::new_honeycomb_telemetry_layer;
use libhoney::Config;
use tracing_subscriber::{filter::LevelFilter, layer::SubscriberExt, registry};


pub fn setup_honeycomb() {
    let honeycomb_config = Config {
        options: libhoney::client::Options {
            api_key: var("HONEYCOMB_API_KEY").unwrap_or("a1b2AEAf9TlDKVrG79BucC".to_string()),
            dataset: var("HONEYCOMB_DATASET").unwrap_or("draft-dev".to_string()),
            ..Default::default()
        },
        transmission_options: libhoney::transmission::Options::default()
    };

    let telemetry_layer = new_honeycomb_telemetry_layer(
        "draft-dev-service",
        honeycomb_config
    );

    let subscriber = registry::Registry::default()
    .with(LevelFilter::INFO)
    .with(tracing_subscriber::fmt::Layer::default())
    .with(telemetry_layer);

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set global default subscriber");
}