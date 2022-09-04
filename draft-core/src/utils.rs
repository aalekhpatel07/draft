use std::sync::Once;
use color_eyre;

static INIT: Once = Once::new();

pub fn set_up_logging() {
    INIT.call_once(|| {
        color_eyre::install().expect("Failed to install color_eyre.");
        
    })
}