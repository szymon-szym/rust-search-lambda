use lambda_http::{run, service_fn, Body, Error, Request, RequestExt, Response};
use tantivy::{Index, schema::{Schema, TEXT, STORED, INDEXED}, query::QueryParser, collector::TopDocs};

/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
async fn function_handler(event: Request, index: &Index) -> Result<Response<Body>, Error> {

    // Extract some useful information from the request

    let query_input = event
        .query_string_parameters_ref()
        .and_then(|params| params.first("q"))
        .unwrap();

    tracing::info!("query input: {}", query_input);

    // schema builder 

    let mut schema_builder = Schema::builder();

    schema_builder.add_text_field("id", TEXT | STORED);
    schema_builder.add_text_field("author", TEXT | STORED);

    schema_builder.add_text_field("title", TEXT);

    schema_builder.add_u64_field("num_points", INDEXED | STORED);
    schema_builder.add_u64_field("num_comments", INDEXED | STORED);

    schema_builder.add_text_field("created_at", TEXT);

    let schema = schema_builder.build();

    let title = schema.get_field("title").unwrap();

    tracing::info!("index opened");

    let reader = index.reader_builder().try_into()?;

    let searcher = reader.searcher();

    let query_parser = QueryParser::for_index(&index, vec![title]);

    let query = query_parser.parse_query(query_input)?;

    let top_docs = searcher.search(&query, &TopDocs::with_limit(3))?;

    let docs = top_docs
        .iter()
        .map(|(_, doc_address)| {
            let doc_address = doc_address.to_owned();
            searcher.doc(doc_address).unwrap()
        })
        .collect::<Vec<_>>();

    let ids = docs
        .iter()
        .map(|doc| {
            doc
                .get_first(schema.get_field("id").unwrap())
                .and_then(|value| value.as_text())
        })
        .collect::<Vec<Option<_>>>();

        let resp = Response::builder()
        .status(200)
        .header("content-type", "text/html")
        .body(serde_json::to_string(&ids)?.into())
        .map_err(Box::new)?;
    Ok(resp)
}

#[tokio::main]
async fn main() -> Result<(), Error> {

    // index

    let index_path = std::env::var("PATH_EFS")?;

    let index = Index::open_in_dir(&index_path)?;

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    run(service_fn(|ev| function_handler(ev, &index))).await
}
