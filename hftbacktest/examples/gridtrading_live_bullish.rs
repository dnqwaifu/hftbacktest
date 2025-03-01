use algo::gridtrading;
use chrono::Utc;
use hftbacktest::{
    live::{
        ipc::iceoryx::IceoryxUnifiedChannel,
        Instrument,
        LiveBot,
        LiveBotBuilder,
        LoggingRecorder,
    },
    prelude::{Bot, ErrorKind, HashMapMarketDepth}, types::Status,
};
use tracing::error;

mod algo;

fn prepare_live() -> LiveBot<IceoryxUnifiedChannel, HashMapMarketDepth> {
    let mut hbt = LiveBotBuilder::new()
        .register(Instrument::new(
            "bullish",
            "BTCUSDC",
            0.1,
            0.001,
            HashMapMarketDepth::new(0.1, 0.001),
            0,
        ))
        .error_handler(|error| {
            match error.kind {
                ErrorKind::ConnectionInterrupted => {
                    error!("ConnectionInterrupted");
                }
                ErrorKind::CriticalConnectionError => {
                    error!("CriticalConnectionError");
                }
                ErrorKind::OrderError => {
                    let error = error.value();
                    error!(?error, "OrderError");
                }
                ErrorKind::Custom(errno) => {
                    error!(%errno, "custom");
                }
            }
            Ok(())
        })
        .order_recv_hook(|req, resp| {
            if (req.req == Status::New || req.req == Status::Canceled) && (resp.req == Status::None)
            {
                tracing::info!(
                    req_timestamp = req.local_timestamp,
                    exch_timestamp = resp.exch_timestamp,
                    resp_timestamp = Utc::now().timestamp_nanos_opt().unwrap(),
                    req = ?req.req,
                    "Order response is received."
                );
            }
            Ok(())
        })
        .build()
        .unwrap();

    hbt
}

fn main() {
    tracing_subscriber::fmt::init();

    let mut hbt = prepare_live();

    let relative_half_spread = 0.0001;
    let relative_grid_interval = 0.0001;
    let grid_num = 2;
    let min_grid_step = 0.1; // tick size
    let skew = relative_half_spread / grid_num as f64;
    let order_qty = 0.001;
    let max_position = grid_num as f64 * order_qty;

    let mut recorder = LoggingRecorder::new();
    gridtrading(
        &mut hbt,
        &mut recorder,
        relative_half_spread,
        relative_grid_interval,
        grid_num,
        min_grid_step,
        skew,
        order_qty,
        max_position,
    )
    .unwrap();
    hbt.close().unwrap();
}
