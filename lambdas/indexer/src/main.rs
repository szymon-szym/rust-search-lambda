use aws_config::meta::region::RegionProviderChain;
use aws_lambda_events::event::cloudwatch_events::CloudWatchEvent;
use aws_sdk_s3::Client;
use futures::stream::StreamExt;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde::{Deserialize, Serialize};
use tantivy::{
    doc,
    schema::{Schema, INDEXED, STORED, TEXT},
    Index,
};

#[derive(Debug, Deserialize, Serialize)]
struct Post {
    id: String,
    title: String,
    url: String,
    num_points: u64,
    num_comments: u64,
    author: String,
    created_at: String,
}

async fn function_handler(
    _: LambdaEvent<CloudWatchEvent>,
    s3_client: &Client,
) -> Result<(), Error> {
    // get posts from s3

    let bucket_name = std::env::var("POSTS_BUCKET_NAME")?;

    tracing::info!("bucket name in use: {}", &bucket_name);

    let prefix = "posts/".to_string();

    tracing::info!("getting list of objects");

    let objects_list = get_objects_list(&s3_client, &bucket_name, &prefix).await?;

    tracing::info!("got list of objects");

    tracing::info!("objects list length: {}", objects_list.len());

    let sanitized_keys = objects_list
        .into_iter()
        .filter(|key| key.ends_with(".json"))
        .collect::<Vec<_>>();

    tracing::info!("objects sanitized list length: {}", sanitized_keys.len());

    // let short_list = sanitized_keys.into_iter().take(100).collect::<Vec<_>>();

    let files_results = futures::stream::iter(sanitized_keys)
        .map(|key| {
            // println!("getting object {}", key);
            let s3_client = &s3_client;
            let bucket = &bucket_name;
            async move { get_object(&s3_client, &bucket, &key).await }
        })
        .buffer_unordered(10)
        .collect::<Vec<_>>()
        .await;

    let files = files_results.into_iter().collect::<Result<Vec<_>, _>>()?;

    println!("got {} files", files.len());

    let posts: Vec<Post> = files
        .iter()
        .map(|file| serde_json::from_slice(file))
        .collect::<Result<Vec<_>, _>>()?;

    tracing::info!("got posts");

    // define schema

    let mut schema_builder = Schema::builder();

    schema_builder.add_text_field("id", TEXT | STORED);
    schema_builder.add_text_field("author", TEXT | STORED);

    schema_builder.add_text_field("title", TEXT);

    schema_builder.add_u64_field("num_points", INDEXED | STORED);
    schema_builder.add_u64_field("num_comments", INDEXED | STORED);

    schema_builder.add_text_field("created_at", TEXT);

    let schema = schema_builder.build();

    let id = schema.get_field("id").unwrap();
    let author = schema.get_field("author").unwrap();
    let title = schema.get_field("title").unwrap();
    let num_points = schema.get_field("num_points").unwrap();
    let num_comments = schema.get_field("num_comments").unwrap();
    let created_at = schema.get_field("created_at").unwrap();

    let index_path = std::env::var("PATH_EFS")?;

    let index = match Index::create_in_dir(&index_path, schema.clone()) {
        Ok(index) => index,
        Err(e) => {
            tracing::warn!("couldn't create index {}", e);
            Index::open_in_dir(&index_path)?
        }
    };

    tracing::info!("index created/ opened");

    let mut index_writer = index.writer(50_000_000)?;

    posts.iter().for_each(|post| {
        _ = index_writer.add_document(doc!(
            id => post.id.to_string(),
            author => post.author.to_string(),
            title => post.title.to_string(),
            num_points => post.num_points,
            num_comments => post.num_comments,
            created_at => post.created_at.to_string(),
        ))
    });

    index_writer.commit()?;

    tracing::info!("index stored: {:?}", index_path);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // create s3 client
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");

    let config = aws_config::from_env().region(region_provider).load().await;

    let s3_client = aws_sdk_s3::Client::new(&config);

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    run(service_fn(|ev: LambdaEvent<CloudWatchEvent>| {
        function_handler(ev, &s3_client)
    }))
    .await
}

async fn get_objects_list(
    s3_client: &aws_sdk_s3::Client,
    bucket: &String,
    prefix: &String,
) -> anyhow::Result<Vec<String>> {
    s3_client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send()
        .collect::<Vec<_>>()
        .await
        .iter()
        .map(|r| {
            r.as_ref()
                .map_err(|e| anyhow::anyhow!(e.to_string()))
                .map(|op| {
                    op.to_owned()
                        .contents()
                        .to_owned()
                        .iter()
                        .map(|o| o.key().unwrap().to_owned())
                        .collect::<Vec<_>>()
                })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|v| v.into_iter().flatten().collect::<Vec<_>>())
}

async fn get_object(
    s3_client: &aws_sdk_s3::Client,
    bucket: &String,
    key: &String,
) -> anyhow::Result<Vec<u8>> {
    let resp = s3_client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    let body = resp.body.collect().await?;
    Ok(body.to_vec())
}
