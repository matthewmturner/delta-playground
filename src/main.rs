use arrow::util::pretty::print_batches;
use datafusion::execution::context::ExecutionContext;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let mut ctx = ExecutionContext::new();
    let table = deltalake::open_table("~/OpenSource/delta-rs/rust/tests/data/simple_table")
        .await
        .unwrap();
    ctx.register_table("demo", Arc::new(table)).unwrap();

    let batches = ctx
        .sql("SELECT * FROM demo")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    print_batches(&batches);
}
