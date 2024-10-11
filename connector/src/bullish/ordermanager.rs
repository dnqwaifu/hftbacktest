use std::{
    collections::{hash_map::Entry, HashMap},
    num::{ParseFloatError, ParseIntError},
    sync::{Arc, Mutex},
    time::SystemTime,
};

use chrono::Utc;
use rand::Rng;
use thiserror::Error;

use crate::{
    prelude::{get_precision, OrdType, Side, TimeInForce},
    types::{Order, Status, Value},
};

use super::{
    msg::{
        rest::{BullishCommand, BullishCommandV3, CommandResponse},
        ws::{PrivateErrorResponse, PrivateTrade},
    },
    Bullish, BullishError,
};

pub type OrderManagerWrapper = Arc<Mutex<OrderManager>>;

#[derive(Error, Debug)]
pub(super) enum HandleError {
    #[error("px qty parse error: {0}")]
    InvalidPxQty(#[from] ParseFloatError),
    #[error("order id parse error: {0}")]
    InvalidOrderId(ParseIntError),
    #[error("prefix unmatched")]
    PrefixUnmatched,
    #[error("order not found")]
    OrderNotFound,
    #[error("req id is invalid")]
    InvalidReqId,
    #[error("asset not found")]
    AssetNotFound,
    #[error("invalid argument")]
    InvalidArg(&'static str),
    #[error("order already exist")]
    OrderAlreadyExist,
    #[error("serde: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("tokio: {0}")]
    TokioError(#[from] tokio_tungstenite::tungstenite::Error),
}

impl Into<Value> for HandleError {
    fn into(self) -> Value {
        // todo!: improve this to deliver detailed error information.
        Value::String(self.to_string())
    }
}

#[derive(Debug)]
struct OrderWrapper {
    asset_no: usize,
    order: Order,
    removed_by_ws: bool,
    removed_by_rest: bool,
}

/// Bullish has separated channels for REST APIs and Websocket. Order responses are delivered
/// through these channels, with no guaranteed order of transmission. To prevent duplicate handling
/// of order responses, such as order deletion due to cancellation or fill, OrderManager manages the
/// order states before transmitting the responses to a live bot.
#[derive(Default, Debug)]
pub struct OrderManager {
    prefix: String,
    orders: HashMap<String, OrderWrapper>,
    order_id_map: HashMap<u64, String>,
}

impl OrderManager {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            orders: Default::default(),
            order_id_map: Default::default(),
        }
    }

    pub fn update_from_ws_error(
        &mut self,
        asset_no: usize,
        client_order_id: &String,
    ) -> Option<Order> {
        match self.orders.entry(client_order_id.clone()) {
            Entry::Occupied(entry) => {
                let wrapper = entry.remove();
                Some(Order {
                    status: Status::Expired,
                    req: Status::None,
                    ..wrapper.order.clone()
                })
            }
            Entry::Vacant(entry) => {
                tracing::debug!(
                    %client_order_id,
                    "Bullish OrderManager received an Error for unmanaged order from WS."
                );
                return None;
            }
        }
    }

    pub fn update_from_ws(
        &mut self,
        asset_no: usize,
        client_order_id: String,
        order: Order,
    ) -> Option<Order> {
        match self.orders.entry(client_order_id.clone()) {
            Entry::Occupied(mut entry) => {
                let wrapper = entry.get_mut();
                let already_removed = wrapper.removed_by_ws || wrapper.removed_by_rest;
                if order.exch_timestamp >= wrapper.order.exch_timestamp {
                    wrapper.order.update(&order);
                }
                if order.status != Status::New && order.status != Status::PartiallyFilled {
                    wrapper.removed_by_ws = true;
                    if !already_removed {
                        self.order_id_map.remove(&order.order_id);
                    }
                    if wrapper.removed_by_ws && wrapper.removed_by_rest {
                        entry.remove_entry();
                    }
                }

                if already_removed {
                    None
                } else {
                    Some(order)
                }
            }
            Entry::Vacant(entry) => {
                if !order.active() {
                    return None;
                }

                tracing::debug!(
                    %client_order_id,
                    ?order,
                    "Bullish OrderManager received an unmanaged order from WS."
                );
                let wrapper = entry.insert(OrderWrapper {
                    asset_no,
                    order: order.clone(),
                    removed_by_ws: order.status != Status::New
                        && order.status != Status::PartiallyFilled,
                    removed_by_rest: false,
                });
                if wrapper.removed_by_ws || wrapper.removed_by_rest {
                    self.order_id_map.remove(&order.order_id);
                }
                Some(order)
            }
        }
    }

    pub fn update_from_rest(
        &mut self,
        asset_no: usize,
        client_order_id: String,
        order: Order,
    ) -> Option<Order> {
        match self.orders.entry(client_order_id.clone()) {
            Entry::Occupied(mut entry) => {
                let wrapper = entry.get_mut();
                let already_removed = wrapper.removed_by_ws || wrapper.removed_by_rest;
                if order.exch_timestamp >= wrapper.order.exch_timestamp {
                    wrapper.order.update(&order);
                }

                if order.status != Status::New && order.status != Status::PartiallyFilled {
                    wrapper.removed_by_rest = true;
                    if !already_removed {
                        self.order_id_map.remove(&order.order_id);
                    }

                    if wrapper.removed_by_ws && wrapper.removed_by_rest {
                        entry.remove_entry();
                    }
                }

                if already_removed {
                    None
                } else {
                    Some(order)
                }
            }
            Entry::Vacant(entry) => {
                if !order.active() {
                    return None;
                }

                tracing::debug!(
                    %client_order_id,
                    ?order,
                    "Bullish OrderManager received an unmanaged order from REST."
                );
                let wrapper = entry.insert(OrderWrapper {
                    asset_no,
                    order: order.clone(),
                    removed_by_ws: false,
                    removed_by_rest: order.status != Status::New
                        && order.status != Status::PartiallyFilled,
                });
                if wrapper.removed_by_ws || wrapper.removed_by_rest {
                    self.order_id_map.remove(&order.order_id);
                }
                Some(order)
            }
        }
    }

    /*
        Bullish order response is just an ack, no new information besides
        exchange order_id.
    */
    pub fn update_submit_success(
        &mut self,
        asset_no: usize,
        order: Order,
        client_order_id: String,
    ) -> Option<Order> {
        let order = Order {
            qty: order.qty,
            leaves_qty: order.qty,
            price_tick: order.price_tick,
            tick_size: order.tick_size,
            side: order.side,
            time_in_force: order.time_in_force,
            exch_timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64,
            status: Status::New,
            local_timestamp: 0,
            req: Status::None,
            exec_price_tick: 0,
            exec_qty: 0.0,
            order_id: order.order_id,
            order_type: order.order_type,
            // Invalid information
            q: Box::new(()),
            maker: false,
        };
        self.update_from_rest(asset_no, client_order_id, order)
    }

    pub fn update_submit_fail(
        &mut self,
        asset_no: usize,
        mut order: Order,
        error: &BullishError,
        client_order_id: String,
    ) -> Option<Order> {
        match error {
            &BullishError::OrderError(-5022, _) => {
                // GTX rejection.
            }
            BullishError::OrderError(-1008, _) => {
                // Server is currently overloaded with other requests. Please try again in a few minutes.
                tracing::error!("Server is currently overloaded with other requests. Please try again in a few minutes.");
            }
            &BullishError::OrderError(-2019, _) => {
                // Margin is insufficient.
                tracing::error!("Margin is insufficient.");
            }
            &BullishError::OrderError(-1015, _) => {
                // Too many new orders; current limit is ?????
                tracing::error!("Too many new orders; current limit is ????.");
            }
            error => {
                tracing::error!(?error, "submit error");
            }
        }

        order.req = Status::None;
        order.status = Status::Expired;
        self.update_from_rest(asset_no, client_order_id, order)
    }

    pub fn update_cancel_success(
        &mut self,
        asset_no: usize,
        mut order: Order,
        client_order_id: String,
    ) -> Option<Order> {
        let order = Order {
            qty: order.qty,
            leaves_qty: order.leaves_qty,
            price_tick: order.price_tick,
            tick_size: order.tick_size,
            side: order.side,
            time_in_force: order.time_in_force,
            exch_timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64,
            status: Status::Canceled,
            local_timestamp: 0,
            req: Status::None,
            exec_price_tick: 0,
            exec_qty: order.exec_qty,
            order_id: order.order_id,
            order_type: order.order_type,
            // Invalid information
            q: Box::new(()),
            maker: false,
        };
        self.update_from_rest(asset_no, client_order_id, order)
    }

    pub fn update_cancel_fail(
        &mut self,
        asset_no: usize,
        mut order: Order,
        error: &BullishError,
        client_order_id: String,
    ) -> Option<Order> {
        match error {
            &BullishError::OrderError(-2011, _) => {
                // The given order may no longer exist; it could have already been filled or
                // canceled. But, it cannot determine the order status because it lacks the
                // necessary information.
                order.leaves_qty = 0.0;
                order.status = Status::None;
            }
            error => {
                tracing::error!(?error, "cancel error");
            }
        }
        order.req = Status::None;
        self.update_from_rest(asset_no, client_order_id, order)
    }

    pub fn prepare_client_order_id(&mut self, asset_no: usize, order: Order) -> Option<String> {
        if self.order_id_map.contains_key(&order.order_id) {
            return None;
        }

        let rand_id = gen_random_digits(8);

        let client_order_id = format!("{}{}{}", self.prefix, &rand_id, order.order_id);
        if self.orders.contains_key(&client_order_id) {
            return None;
        }

        self.order_id_map
            .insert(order.order_id, client_order_id.clone());
        self.orders.insert(
            client_order_id.clone(),
            OrderWrapper {
                asset_no,
                order,
                removed_by_ws: false,
                removed_by_rest: false,
            },
        );
        Some(client_order_id)
    }

    pub fn get_client_order_id(&self, order_id: u64) -> Option<String> {
        self.order_id_map.get(&order_id).cloned()
    }

    pub fn gc(&mut self) {
        let now = Utc::now().timestamp_nanos_opt().unwrap();
        let stale_ts = now - 300_000_000_000;
        let stale_ids: Vec<(_, _)> = self
            .orders
            .iter()
            .filter(|&(_, wrapper)| {
                wrapper.order.status != Status::New
                    && wrapper.order.status != Status::PartiallyFilled
                    && wrapper.order.status != Status::Unsupported
                    && wrapper.order.exch_timestamp < stale_ts
            })
            .map(|(client_order_id, wrapper)| (client_order_id.clone(), wrapper.order.order_id))
            .collect();
        for (client_order_id, order_id) in stale_ids.iter() {
            if self.order_id_map.contains_key(order_id) {
                // Something went wrong?
            }
            self.orders.remove(client_order_id);
        }
    }

    pub fn parse_client_order_id(client_order_id: &str, prefix: &str) -> Option<u64> {
        if !client_order_id.starts_with(prefix) {
            None
        } else {
            let s = &client_order_id[(prefix.len() + 8)..];
            if let Ok(order_id) = s.parse() {
                Some(order_id)
            } else {
                None
            }
        }
    }

    pub fn clear_orders(&mut self) -> Vec<(usize, Order)> {
        let mut values: Vec<(usize, Order)> = Vec::new();
        values.extend(self.orders.drain().map(|(_, mut order)| {
            order.order.status = Status::Canceled;
            (order.asset_no, order.order)
        }));
        values
    }

    fn parse_order_id(&self, order_link_id: &str) -> Result<i64, HandleError> {
        if !order_link_id.starts_with(&self.prefix) {
            return Err(HandleError::PrefixUnmatched);
        }
        order_link_id[(self.prefix.len() + 8)..]
            .parse()
            .map_err(|e| HandleError::InvalidOrderId(e))
    }
}

pub fn gen_random_digits(len: usize) -> String {
    let mut rng = rand::thread_rng();
    (0..len).map(|_| rng.gen_range(0..10).to_string()).collect()
}
