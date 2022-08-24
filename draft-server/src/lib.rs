mod utils;
pub use utils::*;

use draft_core::{AppendEntriesRequest, VoteRequest};
use libhoney::{transmission::Transmission, Client};
use tokio::sync::mpsc;
use tracing_honeycomb::{
    new_honeycomb_telemetry_layer_with_trace_sampling, HoneycombTelemetry, SpanId, TelemetryLayer,
    TraceId,
};

#[derive(Debug, Clone)]
pub struct RPCSenderChannels {
    pub request_vote: mpsc::UnboundedSender<VoteRequest>,
    pub append_entries: mpsc::UnboundedSender<AppendEntriesRequest>,
}

pub fn setup_logging(
) -> TelemetryLayer<HoneycombTelemetry<std::sync::Mutex<Client<Transmission>>>, SpanId, TraceId> {
    let service_name = "draft-server";
    let honeycomb_config = libhoney::Config {
        options: libhoney::client::Options {
            api_key: "a1b2AEAf9TlDKVrG79BucC".to_owned(),
            api_host: "https://api.honeycomb.io".to_owned(),
            ..Default::default()
        },
        transmission_options: libhoney::transmission::Options::default(),
    };
    let sample_rate = 1000;
    new_honeycomb_telemetry_layer_with_trace_sampling(service_name, honeycomb_config, sample_rate)
}
