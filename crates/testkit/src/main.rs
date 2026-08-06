use relay_testkit::Receiver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let secret = std::env::var("RELAY_TESTKIT_SECRET").unwrap_or_else(|_| "whsec_test".into());
    let bind = std::env::var("RELAY_TESTKIT_BIND").unwrap_or_else(|_| "0.0.0.0:9090".into());

    let receiver = Receiver::new(secret);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    println!("testkit receiver on {bind}");
    axum::serve(listener, receiver.router()).await?;
    Ok(())
}
