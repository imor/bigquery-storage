use std::error::Error;

use bigquery_storage::WriteClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(e) = main_impl().await {
        println!("error: {e}");
    }

    Ok(())
}

async fn main_impl() -> Result<(), Box<dyn Error>> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // 2. Load the desired secret (here, a service account key)
    let sa_key = yup_oauth2::read_service_account_key(
        "/Users/raminder.singh/Downloads/pg-replicate-test-project-key-2d4fd0720f29.json",
    )
    .await?;

    // 3. Create an Authenticator
    let auth = yup_oauth2::ServiceAccountAuthenticator::builder(sa_key)
        .build()
        .await?;
    let mut client = WriteClient::new(auth).await?;
    client.append_rows().await?;

    Ok(())
}
