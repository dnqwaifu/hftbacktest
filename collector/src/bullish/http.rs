use std::{
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::{select, sync::mpsc::{unbounded_channel, UnboundedSender}};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use tracing::{error, info, warn};

pub async fn fetch_symbol_list() -> Result<Vec<String>, reqwest::Error> {
    Ok(reqwest::Client::new()
        .get("https://exchange.bullish.com/trading-api/v1/markets")
        .header("Accept", "application/json")
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?
        .as_array()
        .unwrap()
        .iter()
        .map(|market_config| {
           market_config 
                .get("symbol")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect())
}

pub async fn fetch_depth_snapshot(symbol: &str) -> Result<String, reqwest::Error> {
    reqwest::Client::new()
        .get(format!(
             "https://exchange.bullish.com/trading-api/v1/markets/{symbol}/orderbook/hybrid"
        ))
        .header("Accept", "application/json")
        .send()
        .await?
        .text()
        .await
}


/*
pub async fn connect_trades(
    url: &str,
    ev_tx: Sender<LiveEvent>,
    assets: HashMap<String, Asset>,
    topics: Vec<String>,
) -> Result<(), HandleError> {
    let mut request = url.into_client_request()?;
    let _ = request.headers_mut();
    let (ws_stream, _) = connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();
    tracing::trace!("Connected to public trades...");

    let mut interval = time::interval(Duration::from_secs(15));

    let mut params = Vec::new();
    for topic in topics {
        let mut topics_ = assets
            .keys()
            .map(|symbol| {
                HashMap::from([
                    ("topic".to_string(), topic.to_string()),
                    ("symbol".to_string(), symbol.to_string()),
                ])
            })
            .collect();
        params.append(&mut topics_);
    }
    trace!(?params, "Params");

    for param in params {
        let sub = Subscription {
            jsonrpc: "2.0",
            r#type: "command",
            method: "subscribe",
            id: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_string(),
            params: param,
        };
        let s = serde_json::to_string(&sub).unwrap();
        trace!(?sub, "Sending Subscription Message");
        write.send(Message::Text(s)).await?;
    }

    let heartbeat = Subscription {
        jsonrpc: "2.0",
        r#type: "command",
        method: "subscribe",
        id: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string(),
        params: HashMap::from([("topic".to_string(), "heartbeat".to_string())]),
    };
    let s = serde_json::to_string(&heartbeat).unwrap();
    write.send(Message::Text(s)).await?;

    loop {
        select! {
            _ = interval.tick() => {
                let op = Ping {
                  jsonrpc: "2.0",
                  r#type: "command",
                  method: "keepalivePing",
                  id: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().to_string(),
                  params: HashMap::with_capacity(0),
                };
                let s = serde_json::to_string(&op).unwrap();
                write.send(Message::Text(s)).await?;
            },
            message = read.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        if let Err(error) = handle_trades_stream(&text, &ev_tx, &assets).await {
                            error!("Couldn't handle TradesStreamMsg. {:?} {:?}", error, text);
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {}
                    Some(Ok(Message::Ping(_))) => {
                        write.send(Message::Pong(Vec::new())).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(close_frame))) => {
                        warn!(?close_frame, "close");
                        break;
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(e)) => {
                        return Err(HandleError::from(e));
                    }
                    None => {
                        break;
                    }
                }
            }
        }
    }
    tracing::debug!("Exiting");
    Ok(())
}
*/

//let mut topics = vec!["anonymousTrades".to_string()];
pub async fn connect(
    url: &str,
    topics: Vec<String>,
    ws_tx: UnboundedSender<(DateTime<Utc>, String)>,
) -> Result<(), anyhow::Error> {
    let request = url.into_client_request()?;
    let (ws_stream, _) = connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();
    let (tx, mut rx) = unbounded_channel::<()>();

    for topic in topics {
       write.send( Message::Text(format!(
                r#"{{"jsonrpc": "2.0", "type": "command", "method": "subscribe", "id": "{}", "params": {{ {} }}}}"#,
                Utc::now().timestamp_millis(),
                topic 
            ))
        ).await?;
    }
    tokio::spawn(async move {
        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            select! {
                result = rx.recv() => {
                    match result {
                        Some(_) => {
                            if write.send(Message::Pong(Vec::new())).await.is_err() {
                                return;
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }
                _ = ping_interval.tick() => {
                    let ping = Message::Text(format!(r#"{{"jsonrpc": "2.0", "type": "command", "method": "keepalivePing", "id": "{}", "params": {{}}}}"#,Utc::now().timestamp_millis()));
                    if write.send(
                        ping
                    ).await.is_err() {
                        return;
                    }
                }
            }
        }
    });

    loop {
        match read.next().await {
            Some(Ok(Message::Text(text))) => {
                let recv_time = Utc::now();
                if ws_tx.send((recv_time, text)).is_err() {
                    break;
                }
            }
            Some(Ok(Message::Binary(_))) => {}
            Some(Ok(Message::Ping(_))) => {
                tx.send(()).unwrap();
            }
            Some(Ok(Message::Pong(_))) => {}
            Some(Ok(Message::Close(close_frame))) => {
                warn!(?close_frame, "closed");
                return Err(Error::from(io::Error::new(
                    ErrorKind::ConnectionAborted,
                    "closed",
                )));
            }
            Some(Ok(Message::Frame(_))) => {}
            Some(Err(e)) => {
                return Err(Error::from(e));
            }
            None => {
                break;
            }
        }
    }
    Ok(())
}

pub async fn keep_connection(
    topics: Vec<String>,
    symbol_list: Vec<String>,
    ws_tx: UnboundedSender<(DateTime<Utc>, String)>,
) {
    let mut error_count = 0;
    loop {
        let connect_time = Instant::now();
        // jsonrpc data like {"topic":"AnonymousTrades","symbol":"BTCUSDC"}
        let topics_ = symbol_list
            .iter()
            .flat_map(|pair| {
                topics
                    .iter()
                    .cloned()
                    .map(|stream| {
                        stream
                            .replace("$symbol", pair.to_uppercase().as_str())
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if let Err(error) = connect(
            "wss://exchange.bullish.com/trading-api/v1/market-data/trades",
            topics_,
            ws_tx.clone(),
        )
        .await
        {
            error!(?error, "websocket error");
            error_count += 1;
            if connect_time.elapsed() > Duration::from_secs(30) {
                error_count = 0;
            }
            if error_count > 3 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else if error_count > 10 {
                tokio::time::sleep(Duration::from_secs(5)).await;
            } else if error_count > 20 {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        } else {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fetch_symbol_list() {
        let got = fetch_symbol_list().await.unwrap();
        assert!(got.len() > 0);
    }
}