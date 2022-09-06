use color_eyre;
use std::sync::Once;

static INIT: Once = Once::new();

pub fn set_up_logging() {
    INIT.call_once(|| {
        color_eyre::install().expect("Failed to install color_eyre.");
    })
}
