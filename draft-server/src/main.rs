use std::path::PathBuf;

use draft_core::{
    BufferBackend, 
    config::load_from_file, 
};
use draft_server::RaftRuntime;
use tokio::net::UdpSocket;
use tracing::{info_span, Instrument, Level};
use structopt::StructOpt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(StructOpt)]
#[structopt(name="Raft Server.", about = "The runtime for Raft. (without the state-machine backend for now).")]
pub struct Opt {
    #[structopt(short, long, parse(from_os_str))]
    config: PathBuf,

    #[structopt(short, long, parse(from_occurrences), default_value = "0")]
    verbose: u8,
}


#[tokio::main]
pub async fn main() -> color_eyre::Result<()> {

    let opt = Opt::from_args();

    let log_level = {
        match opt.verbose {
            0 => Level::INFO,
            1 => Level::DEBUG,
            _ => Level::TRACE
        }
    };

    // Set up the Jaeger sender
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name("draft")
        .install_batch(opentelemetry::runtime::Tokio)?;

    // The OpenTelemetry layer that listens for tracing calls and turns them into OpenTelemetry requests
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    // The layer that dumps logs to the console
    let console_layer = tracing_subscriber::fmt::layer().pretty();

    // The tracing subscriber that arranges for our traces to go through the layers we want
    let subscriber = Registry::default()
        .with(otel_layer)
        .with(console_layer);

    // Install the subscriber as the global default
    subscriber.init();

    let config = load_from_file(opt.config)?;
    let raft: RaftRuntime<BufferBackend, UdpSocket> = RaftRuntime::new(config);
    raft.run().instrument(info_span!("app")).await?;

    // Cleanly shut down opentelemetry so all queued requests can be sent out
    opentelemetry::global::shutdown_tracer_provider();

    Ok(())
}
