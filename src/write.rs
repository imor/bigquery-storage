use std::collections::HashMap;

use tonic::transport::{Channel, ClientTlsConfig};

use crate::{
    google::cloud::bigquery::storage::v1::{
        append_rows_request::MissingValueInterpretation,
        big_query_write_client::BigQueryWriteClient, AppendRowsRequest,
    },
    Error, API_DOMAIN, API_ENDPOINT,
};

async fn new_write_client() -> Result<BigQueryWriteClient<Channel>, Error> {
    let tls_config = ClientTlsConfig::new().domain_name(API_DOMAIN);
    let channel = Channel::from_static(API_ENDPOINT)
        .tls_config(tls_config)?
        .connect()
        .await?;
    let write_client = BigQueryWriteClient::new(channel);

    Ok(write_client)
}

pub async fn append_rows() -> Result<(), Error> {
    let mut client = new_write_client().await?;

    let project = "pg-replicate-test-project";
    let dataset = "test_data_set";
    let table = "actor";

    let write_stream =
        format!("projects/{project}/datasets/{dataset}/tables/{table}/streams/_default");

    let trace_id = "test_client".to_string();

    let append_rows_request = AppendRowsRequest {
        write_stream,
        offset: None,
        trace_id,
        missing_value_interpretations: HashMap::new(),
        default_missing_value_interpretation: MissingValueInterpretation::Unspecified.into(),
        rows: None,
    };

    let _response = client
        .append_rows(tokio_stream::iter(vec![append_rows_request]))
        .await?;

    Ok(())
}
