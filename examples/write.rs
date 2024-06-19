use std::error::Error;

use bigquery_storage::append_rows;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(e) = main_impl().await {
        println!("error: {e}");
    }

    Ok(())
}

async fn main_impl() -> Result<(), Box<dyn Error>> {
    append_rows().await?;
    Ok(())
}
