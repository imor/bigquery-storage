use std::{collections::HashMap, convert::TryInto};

use hyper_util::client::legacy::connect::Connect;
use prost::Message;
use prost_types::{field_descriptor_proto::Type, DescriptorProto, FieldDescriptorProto};
use tokio_stream::StreamExt;
use tonic::{
    transport::{Channel, ClientTlsConfig},
    Request,
};
use yup_oauth2::authenticator::Authenticator;

use crate::{
    google::cloud::bigquery::storage::v1::{
        append_rows_request::{self, MissingValueInterpretation, ProtoData},
        big_query_write_client::BigQueryWriteClient,
        AppendRowsRequest, ProtoSchema,
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
            rows: Some(Self::create_rows()),
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

    fn create_rows() -> append_rows_request::Rows {
        let proto_descriptor = DescriptorProto {
            name: Some("table_schema".to_string()),
            field: vec![
                FieldDescriptorProto {
                    name: Some("actor_id".to_string()),
                    number: Some(1),
                    label: None,
                    r#type: Some(Type::Int64.into()),
                    type_name: None,
                    extendee: None,
                    default_value: None,
                    oneof_index: None,
                    json_name: None,
                    options: None,
                    proto3_optional: None,
                },
                FieldDescriptorProto {
                    name: Some("first_name".to_string()),
                    number: Some(2),
                    label: None,
                    r#type: Some(Type::String.into()),
                    type_name: None,
                    extendee: None,
                    default_value: None,
                    oneof_index: None,
                    json_name: None,
                    options: None,
                    proto3_optional: None,
                },
                FieldDescriptorProto {
                    name: Some("last_name".to_string()),
                    number: Some(3),
                    label: None,
                    r#type: Some(Type::String.into()),
                    type_name: None,
                    extendee: None,
                    default_value: None,
                    oneof_index: None,
                    json_name: None,
                    options: None,
                    proto3_optional: None,
                },
                FieldDescriptorProto {
                    name: Some("last_update".to_string()),
                    number: Some(4),
                    label: None,
                    r#type: Some(Type::String.into()),
                    type_name: None,
                    extendee: None,
                    default_value: None,
                    oneof_index: None,
                    json_name: None,
                    options: None,
                    proto3_optional: None,
                },
            ],
            extension: vec![],
            nested_type: vec![],
            enum_type: vec![],
            extension_range: vec![],
            oneof_decl: vec![],
            options: None,
            reserved_range: vec![],
            reserved_name: vec![],
        };
        let proto_schema = ProtoSchema {
            proto_descriptor: Some(proto_descriptor),
        };

        #[derive(Clone, PartialEq, Message)]
        struct Actor {
            #[prost(int32, tag = "1")]
            actor_id: i32,

            #[prost(string, tag = "2")]
            first_name: String,

            #[prost(string, tag = "3")]
            last_name: String,

            #[prost(string, tag = "4")]
            last_update: String,
        }

        let actor = Actor {
            actor_id: 400,
            first_name: "Raminder".to_string(),
            last_name: "Singh".to_string(),
            last_update: "2006-02-15 09:34:33 UTC".to_string(),
        };

        let actor = actor.encode_to_vec();

        let proto_rows = crate::google::cloud::bigquery::storage::v1::ProtoRows {
            //TODO:add rows
            serialized_rows: vec![actor],
        };

        let proto_data = ProtoData {
            writer_schema: Some(proto_schema),
            rows: Some(proto_rows),
        };
        append_rows_request::Rows::ProtoRows(proto_data)
    }
}
