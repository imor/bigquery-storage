use std::{collections::HashMap, convert::TryInto};

use hyper_util::client::legacy::connect::Connect;
use tokio_stream::StreamExt;
use tonic::{
    transport::{Channel, ClientTlsConfig},
    Request,
};
use yup_oauth2::authenticator::Authenticator;

use crate::{
    google::cloud::bigquery::storage::v1::{
        append_rows_request::MissingValueInterpretation,
        big_query_write_client::BigQueryWriteClient, AppendRowsRequest,
    },
    Error, API_DOMAIN, API_ENDPOINT, API_SCOPE,
};

pub struct WriteClient<C> {
    auth: Authenticator<C>,
    inner: BigQueryWriteClient<Channel>,
}

impl<C> WriteClient<C>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    pub async fn new(auth: Authenticator<C>) -> Result<Self, Error> {
        let tls_config = ClientTlsConfig::new().domain_name(API_DOMAIN);
        let channel = Channel::from_static(API_ENDPOINT)
            .tls_config(tls_config)?
            .connect()
            .await?;
        let inner = BigQueryWriteClient::new(channel);

        Ok(WriteClient { auth, inner })
    }

    async fn new_request<D>(&self, t: D) -> Result<Request<D>, Error> {
        let token = self.auth.token(&[API_SCOPE]).await?;
        let bearer_token = format!(
            "Bearer {}",
            token
                .token()
                .ok_or(Error::Auth(yup_oauth2::Error::MissingAccessToken))?
        );
        let bearer_value = bearer_token.as_str().try_into()?;
        let mut req = Request::new(t);
        let meta = req.metadata_mut();
        meta.insert("authorization", bearer_value);
        // meta.insert("x-goog-request-params", params.try_into()?);
        Ok(req)
    }

    pub async fn append_rows(&mut self) -> Result<(), Error> {
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

        let req = self
            .new_request(tokio_stream::iter(vec![append_rows_request]))
            .await?;

        let response = self.inner.append_rows(req).await?;

        println!("streaming response: {response:#?}");

        let mut streaming = response.into_inner();

        while let Some(resp) = streaming.next().await {
            let resp = resp?;
            println!("response: {resp:#?}");
        }

        Ok(())
    }
}
