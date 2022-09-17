use tracing::Level;
use std::sync::Once;

static INIT: Once = Once::new();

pub fn set_up_logging(level: Level) {
    INIT.call_once(|| {
        color_eyre::install().expect("Failed to install color_eyre.");

        tracing_subscriber::fmt()
        .with_max_level(level)
        .init();

    })
}