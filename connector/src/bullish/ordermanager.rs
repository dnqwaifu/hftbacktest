use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};


use hftbacktest::types::{Order, Status, OrderId};

use crate::{
    connector::GetOrders,
    utils::{generate_rand_digits, SymbolOrderId},
};

use super::{
    msg::{
        rest::CommandResponse,
        ws::PrivateOrder,
    }, BullishError,
};

#[derive(Debug)]
struct OrderExt {
    symbol: String,
    order: Order,
    removed_by_ws: bool,
    removed_by_rest: bool,
}

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;

pub type ClientOrderId = String;

/// Bullish has separate channels for REST APIs and Websocket. Order responses are delivered
/// through these channels, with no guaranteed order of transmission. To prevent duplicate handling
/// of order responses, such as order deletion due to cancellation or fill, OrderManager manages the
/// order states before transmitting the responses to a live bot.
///
/// Deletions must be confirmed by both channels. If not, differences in response times could result
/// in attempts to update an order that has already been deleted, potentially creating a ghost order
/// unintentionally.
///
/// To handle this, the `client_order_id` should include a random ID to differentiate it, even when
/// the order ID is the same(bot's order id). This is necessary because the order deletion is
/// immediately notified to the bot, but the Connector must still retain the `client_order_id` in
/// case an update arrives later from the other channel, which has not yet sent the deletion
/// message.
#[derive(Default, Debug)]
pub struct OrderManager {
    prefix: String,
    orders: HashMap<ClientOrderId, OrderExt>,
    order_id_map: HashMap<SymbolOrderId, ClientOrderId>,
}

impl OrderManager {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            orders: Default::default(),
            order_id_map: Default::default(),
        }
    }

    pub fn update_from_ws(
        &mut self,
        resp: &PrivateOrder,
    ) -> Result<Option<Order>, BullishError> {
        if let Some(client_order_id) = resp.handle.clone() {
            if !client_order_id.starts_with(&self.prefix) {
                return Err(BullishError::PrefixUnmatched);
            }

            let order_ext = self.orders
                .get_mut(&client_order_id)
                .ok_or(BullishError::OrderNotFound)?;

            let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
            if resp.created_at_timestamp * 1_000_000 >= order_ext.order.exch_timestamp {
                order_ext.order.qty = resp.quantity;
                if let Some(quantity_filled ) = resp.quantity_filled {
                    order_ext.order.leaves_qty = resp.quantity - quantity_filled;
                }
                order_ext.order.side = resp.side;
                order_ext.order.time_in_force = resp.time_in_force;
                order_ext.order.exch_timestamp = resp.created_at_timestamp * 1_000_000;
                order_ext.order.status = resp.status_reason;
                // TODO this may only available from the PrivateTrade
                // order_ext.order.exec_qty
                order_ext.order.order_type = resp.order_type;
            }

            let result = if already_removed {
                None
            } else {
                Some(order_ext.order.clone())
            };

            if order_ext.order.status != Status::New
                && order_ext.order.status != Status::PartiallyFilled
            {
                order_ext.removed_by_ws = true;

                if !already_removed {
                    todo!("This order needs to be removed from the order_id_map");
                    // self.order_id_map.remove(&SymbolOrderId::new(
                    //     &order_ext.symbol.,
                    //     order_ext.order.order_id,
                    // ));
                }
            }

            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                self.orders.remove(&client_order_id).unwrap();
            }

            Ok(result)
        } else {
            Ok(None)
        }
    }

    pub fn update_from_rest(
        &mut self,
        client_order_id: &ClientOrderId,
        resp: &CommandResponse,
    ) -> Option<Order> {
        todo!("This needs to combine the info sent to the command and the command response.");
    }

    pub fn update_cancel_fail(
        &mut self,
        client_order_id: &ClientOrderId,
        error: &BullishError,
    ) -> Option<Order> {
        todo!("This needs to combine the info sent to the command and the command response.");
    }

    pub fn update_submit_fail(
        &mut self,
        client_order_id: &ClientOrderId,
        error: &BullishError,
    ) -> Option<Order> {
        todo!("This needs to combine the info sent to the command and the command response.");
    }

    pub fn prepare_client_order_id(&mut self, symbol: String, order: Order) -> Option<String> {
        let symbol_order_id = SymbolOrderId::new(symbol.clone(), order.order_id);
        if self.order_id_map.contains_key(&symbol_order_id) {
            println!("symbol order id duplicate");
            return None;
        }

        let client_order_id = format!("{}{}", self.prefix, generate_rand_digits(10));
        if self.orders.contains_key(&client_order_id) {
            return None;
        }

        self.order_id_map
            .insert(symbol_order_id, client_order_id.clone());
        self.orders.insert(
            client_order_id.clone(),
            OrderExt {
                symbol,
                order,
                removed_by_ws: false,
                removed_by_rest: false,
            },
        );
        Some(client_order_id)
    }

    pub fn get_client_order_id(&self, symbol: &str, order_id: OrderId) -> Option<String> {
        todo!("Figure out why this is the wrong type");
        // self.order_id_map
        //     .get(&RefSymbolOrderId::new(symbol, order_id))
        //     .cloned()
    }

    pub fn gc(&mut self) {
        todo!("garbage collect");
        /*
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
        */
    }

}

impl GetOrders for OrderManager {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        self.orders
            .iter()
            .filter(|(_, order)| {
                symbol.as_ref().map(|s| order.symbol == *s).unwrap_or(true) && order.order.active()
            })
            .map(|(_, order)| &order.order)
            .cloned()
            .collect()
    }
}