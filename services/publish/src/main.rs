use std::net::SocketAddr;

use axum::{
    Router,
    routing::{get, post},
};
use broker::RedisClient;
use dotenvy::dotenv;
use publish::{
    handlers::{
        health::{healthz_handler, readyz_handler},
        publish::publish_handler,
    },
    state::AppState,
};
use tokio::{net::TcpListener, runtime::Runtime, time::Instant};

fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let runtime = Runtime::new()?;
    runtime.block_on(async_main())
}

async fn async_main() -> anyhow::Result<(), anyhow::Error> {
    let port = std::env::var("PUBLISH_PORT")?;
    let redis_url = std::env::var("REDIS_URL")?;

    let broker = RedisClient::new(redis_url.as_str()).await?;

    let state = AppState {
        redis: broker,
        start_time: Instant::now(),
    };

    let mut app = Router::new()
        .route("/healthz", get(healthz_handler))
        .route("/readyz", get(readyz_handler))
        .route("/publish/{org_id}/{channel_id}", post(publish_handler))
        .with_state(state);

    #[cfg(debug_assertions)]
    {
        use tower_http::cors::{Any, CorsLayer};

        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any);

        app = app.layer(cors);
    }

    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            eprintln!("Shutting down...");
        })
        .await?;

    Ok(())
}
