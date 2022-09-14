use draft_server::set_up_logging;


#[tokio::main]
pub async fn main() -> color_eyre::Result<()> {
    set_up_logging();

    Ok(())
}