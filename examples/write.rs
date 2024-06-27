use std::error::Error;

use bigquery_storage::{ColumnType, FieldDescriptor, StreamName, TableDescriptor, WriteClient};
use prost::Message;
use tokio_stream::StreamExt;

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
    let field_descriptors = vec![
        FieldDescriptor {
            name: "actor_id".to_string(),
            number: 1,
            typ: ColumnType::Int64,
        },
        FieldDescriptor {
            name: "first_name".to_string(),
            number: 2,
            typ: ColumnType::String,
        },
        FieldDescriptor {
            name: "last_name".to_string(),
            number: 3,
            typ: ColumnType::String,
        },
        FieldDescriptor {
            name: "last_update".to_string(),
            number: 4,
            typ: ColumnType::Timestamp,
        },
    ];
    let table_descriptor = TableDescriptor { field_descriptors };

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

    let actor1 = Actor {
        actor_id: 401,
        first_name: "Tajinder".to_string(),
        last_name: "Singh".to_string(),
        last_update: "2007-02-15 09:34:33 UTC".to_string(),
    };

    let actor2 = Actor {
        actor_id: 402,
        first_name: "Gurinder".to_string(),
        last_name: "Singh".to_string(),
        last_update: "2008-02-15 09:34:33 UTC".to_string(),
    };

    let project = "pg-replicate-test-project".to_string();
    let dataset = "test_data_set".to_string();
    let table = "actor".to_string();

    let stream_name = StreamName::new_default(project, dataset, table);
    let trace_id = "test_client".to_string();

    let mut streaming = client
        .append_rows(&stream_name, &table_descriptor, &[actor1, actor2], trace_id)
        .await?;

    while let Some(resp) = streaming.next().await {
        let resp = resp?;
        println!("response: {resp:#?}");
    }

    Ok(())
}
