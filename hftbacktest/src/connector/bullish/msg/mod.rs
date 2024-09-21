use serde::{
    de::{Error, Unexpected, Visitor},
    Deserialize, Deserializer, Serialize,
};

use crate::{
    connector::util::{from_str_to_f64},
    types::{OrdType, Side, Status, TimeInForce},
};

use std::{
    collections::HashMap,
    fmt::{self, Debug},
    num::{NonZeroI32, NonZeroU32},
};

pub mod rest;
pub mod ws;

// Bullish API can be kind of whacky & inconsistent, status codes can be
// either a string or a number.
fn deserialize_i32_from_string_or_int<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: Deserializer<'de>,
{
    struct StringOrIntVisitor;

    impl<'de> Visitor<'de> for StringOrIntVisitor {
        type Value = i32;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an integer or a string representing an integer")
        }

        fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(value)
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            value.parse::<i32>().map_err(Error::custom)
        }
    }

    deserializer.deserialize_any(StringOrIntVisitor)
}

fn from_str_to_side<'de, D>(deserializer: D) -> Result<Side, D::Error>
where
    D: Deserializer<'de>,
{
    struct StringOrStrVisitor;

    impl<'de> Visitor<'de> for StringOrStrVisitor {
        type Value = Side;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string or str representing either 'BUY' or 'SELL'")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            match value {
                "BUY" => Ok(Side::Buy),
                "SELL" => Ok(Side::Sell),
                _ => Err(Error::invalid_value(Unexpected::Str(value), &self)),
            }
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: Error,
        {
            self.visit_str(&value)
        }
    }

    deserializer.deserialize_str(StringOrStrVisitor)
}

fn from_str_to_status<'de, D>(deserializer: D) -> Result<Status, D::Error>
where
    D: Deserializer<'de>,
{
    struct StringOrStrVisitor;

    impl<'de> Visitor<'de> for StringOrStrVisitor {
        type Value = Status;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string or str representing either 'Order accepted', 'Filled', 'Partially filled', 'Cancelled', or 'Closed'")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            match value {
                "Order accepted" => Ok(Status::New),
                "Open" => Ok(Status::New),
                "Filled" => Ok(Status::Filled),
                "Executed" => Ok(Status::Filled),
                "Partially filled" => Ok(Status::PartiallyFilled),
                "User cancelled" => Ok(Status::Canceled),
                "Cancelled" => Ok(Status::Canceled),
                "Expired" => Ok(Status::Expired),
                "Invalid nonce" => Ok(Status::Expired),
                // "REJECTED" => Ok(Status::Rejected),
                // "EXPIRED_IN_MATCH" => Ok(Status::ExpiredInMatch),
                s => Err(Error::invalid_value(
                    Unexpected::Other(s),
                    &"NEW,PARTIALLY_FILLED,FILLED,CANCELED,EXPIRED",
                )),
            }
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: Error,
        {
            self.visit_str(&value)
        }
    }

    deserializer.deserialize_str(StringOrStrVisitor)
}

fn from_str_to_type<'de, D>(deserializer: D) -> Result<OrdType, D::Error>
where
    D: Deserializer<'de>,
{
    struct StringOrStrVisitor;

    impl<'de> Visitor<'de> for StringOrStrVisitor {
        type Value = OrdType;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter
                .write_str("a string or str representing either 'LIMIT', 'LMT', 'MARKET', 'MKT'")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            match value {
                "LIMIT" => Ok(OrdType::Limit),
                "MARKET" => Ok(OrdType::Market),
                "LMT" => Ok(OrdType::Limit),
                "MKT" => Ok(OrdType::Market),
                "POST_ONLY" => Ok(OrdType::Limit),
                s => Err(Error::invalid_value(
                    Unexpected::Other(s),
                    &"LIMIT,MARKET,LMT,MKT,POST_ONLY",
                )),
            }
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: Error,
        {
            self.visit_str(&value)
        }
    }

    deserializer.deserialize_str(StringOrStrVisitor)
}

fn from_str_to_tif<'de, D>(deserializer: D) -> Result<TimeInForce, D::Error>
where
    D: Deserializer<'de>,
{
    struct StringOrStrVisitor;

    impl<'de> Visitor<'de> for StringOrStrVisitor {
        type Value = TimeInForce;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string or str representing either 'IOC', 'FOK', 'GTC'")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            match value {
                "GTC" => Ok(TimeInForce::GTC),
                "IOC" => Ok(TimeInForce::IOC),
                "FOK" => Ok(TimeInForce::FOK),
                // "POST_ONLY" => Ok(TimeInForce::GTX),
                // "GTD" => Ok(TimeInForce::GTD),
                s => Err(Error::invalid_value(
                    Unexpected::Other(s),
                    &"GTC,IOC,FOK,POST_ONLY(GTX)",
                )),
            }
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: Error,
        {
            self.visit_str(&value)
        }
    }

    deserializer.deserialize_str(StringOrStrVisitor)
}

#[derive(Debug, Deserialize)]
pub struct BullishOrder {
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(rename = "priceLevelQuantity", default)]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price_level_quantity: f64,
}
