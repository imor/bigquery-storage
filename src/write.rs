use std::{collections::HashMap, convert::TryInto, fmt::Display};

use hyper_util::client::legacy::connect::Connect;
use prost::Message;
use prost_types::{field_descriptor_proto::Type, DescriptorProto, FieldDescriptorProto};
use tonic::{
    transport::{Channel, ClientTlsConfig},
    Request, Streaming,
};
use yup_oauth2::authenticator::Authenticator;

use crate::{
    google::cloud::bigquery::storage::v1::{
        append_rows_request::{self, MissingValueInterpretation, ProtoData},
        big_query_write_client::BigQueryWriteClient,
        AppendRowsRequest, AppendRowsResponse, ProtoSchema,
    },
    Error, API_DOMAIN, API_ENDPOINT, API_SCOPE,
};

pub struct WriteClient<C> {
    auth: Authenticator<C>,
    inner: BigQueryWriteClient<Channel>,
}

/// BigQuery column type
#[derive(Clone, Copy)]
pub enum ColumnType {
    Bool,
    Bytes,
    Date,
    Datetime,
    Json,
    Int64,
    Float64,
    String,
    Time,
    Timestamp,
}

impl From<ColumnType> for Type {
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::Bool => Type::Bool,
            ColumnType::Bytes => Type::Bytes,
            ColumnType::Date => Type::String,
            ColumnType::Datetime => Type::String,
            ColumnType::Json => Type::String,
            ColumnType::Int64 => Type::Int64,
            ColumnType::Float64 => Type::Float,
            ColumnType::String => Type::String,
            ColumnType::Time => Type::String,
            ColumnType::Timestamp => Type::String,
        }
    }
}

/// A struct to describe the schema of a field in protobuf
pub struct FieldDescriptor {
    /// Field numbers starting from 1. Each subsequence field should be incremented by 1.
    pub number: u32,

    /// Field name
    pub name: String,

    /// Field type
    pub typ: ColumnType,
}

/// A struct to describe the schema of a table in protobuf
pub struct TableDescriptor {
    /// Descriptors of all the fields
    pub field_descriptors: Vec<FieldDescriptor>,
}

/// A struct representing a stream name
pub struct StreamName {
    /// Name of the project
    project: String,

    /// Name of the dataset
    dataset: String,

    /// Name of the table
    table: String,

    /// Name of the stream
    stream: String,
}

impl StreamName {
    pub fn new(project: String, dataset: String, table: String, stream: String) -> StreamName {
        StreamName {
            project,
            dataset,
            table,
            stream,
        }
    }

    pub fn new_default(project: String, dataset: String, table: String) -> StreamName {
        StreamName {
            project,
            dataset,
            table,
            stream: "_default".to_string(),
        }
    }
}

impl Display for StreamName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let StreamName {
            project,
            dataset,
            table,
            stream,
        } = self;
        f.write_fmt(format_args!(
            "projects/{project}/datasets/{dataset}/tables/{table}/streams/{stream}"
        ))
    }
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
        Ok(req)
    }

    pub async fn append_rows<M: Message>(
        &mut self,
        stream_name: &StreamName,
        table_descriptor: &TableDescriptor,
        rows: &[M],
        trace_id: String,
    ) -> Result<Streaming<AppendRowsResponse>, Error> {
        let write_stream = stream_name.to_string();

        let append_rows_request = AppendRowsRequest {
            write_stream,
            offset: None,
            trace_id,
            missing_value_interpretations: HashMap::new(),
            default_missing_value_interpretation: MissingValueInterpretation::Unspecified.into(),
            rows: Some(Self::create_rows(table_descriptor, rows)),
        };

        let req = self
            .new_request(tokio_stream::iter(vec![append_rows_request]))
            .await?;

        let response = self.inner.append_rows(req).await?;

        let streaming = response.into_inner();

        Ok(streaming)
    }

    fn create_rows<M: Message>(
        table_descriptor: &TableDescriptor,
        rows: &[M],
    ) -> append_rows_request::Rows {
        let field_descriptors = table_descriptor
            .field_descriptors
            .iter()
            .map(|fd| {
                let typ: Type = fd.typ.into();
                FieldDescriptorProto {
                    name: Some(fd.name.clone()),
                    number: Some(fd.number as i32),
                    label: None,
                    r#type: Some(typ.into()),
                    type_name: None,
                    extendee: None,
                    default_value: None,
                    oneof_index: None,
                    json_name: None,
                    options: None,
                    proto3_optional: None,
                }
            })
            .collect();
        let proto_descriptor = DescriptorProto {
            name: Some("table_schema".to_string()),
            field: field_descriptors,
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

        let rows = rows.iter().map(|m| m.encode_to_vec()).collect();

        let proto_rows = crate::google::cloud::bigquery::storage::v1::ProtoRows {
            serialized_rows: rows,
        };

        let proto_data = ProtoData {
            writer_schema: Some(proto_schema),
            rows: Some(proto_rows),
        };
        append_rows_request::Rows::ProtoRows(proto_data)
    }
}
