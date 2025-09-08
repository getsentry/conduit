use tokio::time::Instant;

#[derive(Clone)]
pub struct AppState {
    pub start_time: Instant,
}
