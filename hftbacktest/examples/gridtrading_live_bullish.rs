use algo::gridtrading;
use hftbacktest::{
    connector::bullish::{Bullish, BullishError, Endpoint},
    live::{LiveBot, LoggingRecorder},
    prelude::{Bot, HashMapMarketDepth},
    types::{ErrorKind},
};

mod algo;

const ORDER_PREFIX: &str = "600";

// const api_key: &str = "HMAC-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx";
// const secret: &str = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx";

use tracing::error;

fn prepare_live() -> LiveBot<HashMapMarketDepth> {
    let bullish = Bullish::builder()
        .endpoint(Endpoint::Testnet)
        .orderbook_url(Endpoint::Testnet)
        .trades_url(Endpoint::Testnet)
        .private_url(Endpoint::Testnet)
        .api_key(api_key)
        .secret(secret)
        .order_prefix(ORDER_PREFIX)
        .build()
        .unwrap();

    let mut hbt = LiveBot::builder()
        .register("bullish", bullish)
        .add("bullish", "BTCUSDC", 0.1, 0.00029411)
        .add("bullish", "ETHUSDC", 0.01, 0.00498753)
        //.add("bullish", "SUSHIUSDC", 0.0001, 20000.00000000)
        .depth(|asset| HashMapMarketDepth::new(asset.tick_size, asset.lot_size))
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
        // .order_recv_hook(|req, resp| {
        //     if (req.req == Status::New || req.req == Status::Canceled) && (resp.req == Status::None)
        //     {
        //         tracing::info!(
        //             req_timestamp = req.local_timestamp,
        //             exch_timestamp = resp.exch_timestamp,
        //             resp_timestamp = Utc::now().timestamp_nanos_opt().unwrap(),
        //             req = ?req.req,
        //             "Order response is received."
        //         );
        //     }
        //     Ok(())
        // })
        .build()
        .unwrap();

    hbt.run().unwrap();
    hbt
}

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_file(true)
        .with_line_number(true)
        .init();

    let mut hbt = prepare_live();

    let relative_half_spread = 0.00001;
    let relative_grid_interval = 0.00001;
    let grid_num = 20;
    let min_grid_step = 0.1; // tick size
    let skew = relative_half_spread / grid_num as f64;
    let order_qty = 0.5123;
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