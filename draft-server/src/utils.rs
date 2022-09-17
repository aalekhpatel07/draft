use tracing::Level;
use std::sync::Once;

static INIT: Once = Once::new();

pub fn set_up_logging(level: Level) {
    INIT.call_once(|| {
        color_eyre::install().expect("Failed to install color_eyre.");

        tracing_subscriber::fmt()
        .with_max_level(level)
        .init();

        // let level_filter = match level {
        //     Level::INFO => {
        //         tracing_subscriber::filter::LevelFilter::INFO
        //     },
        //     Level::DEBUG => {
        //         tracing_subscriber::filter::LevelFilter::DEBUG
        //     },
        //     _ => {
        //         tracing_subscriber::filter::LevelFilter::TRACE
        //     }
        // };

        // let layer = tracing_opentelemetry::layer()
        //     .with_tracer(tracer);

        // tracing_subscriber::registry()
        // .with(level_filter)
        // .with(layer)
        // .try_init()
        // .expect("Failed to register tracer with registry.");

        // tracing::info!("Tracing registration complete. Exiting set_up_logging().");
        // tracing_subscriber::registry()
        // .with(
        // ).try_init().unwrap();

    })
}