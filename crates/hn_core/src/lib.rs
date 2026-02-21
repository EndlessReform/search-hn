pub mod db;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// Canonical upstream Hacker News item payload shared across services.
///
/// This type intentionally captures the raw shape returned by the HN Firebase API,
/// including optional fields and `kids` edge ordering.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct HnItem {
    pub id: i64,
    #[serde(default, deserialize_with = "deserialize_option_bool_tolerant")]
    pub deleted: Option<bool>,
    /// Maps the HN payload field named `type` into a Rust-safe identifier.
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub by: Option<String>,
    pub time: Option<i64>,
    pub text: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_bool_tolerant")]
    pub dead: Option<bool>,
    pub parent: Option<i64>,
    pub poll: Option<i64>,
    pub url: Option<String>,
    pub score: Option<i64>,
    pub title: Option<String>,
    pub parts: Option<Vec<i64>>,
    pub descendants: Option<i64>,
    pub kids: Option<Vec<i64>>,
}

/// Deserializes an optional bool while tolerating historical upstream schema drift.
///
/// The Firebase mirror has occasionally emitted malformed bool shapes (for example arrays).
/// We coerce obvious representations and otherwise leave the field as `None` so one field does
/// not poison an entire item payload.
fn deserialize_option_bool_tolerant<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    Ok(value.and_then(value_to_bool_tolerant))
}

fn value_to_bool_tolerant(value: Value) -> Option<bool> {
    match value {
        Value::Bool(flag) => Some(flag),
        Value::Number(number) => number.as_f64().map(|n| n != 0.0),
        Value::String(text) => {
            let normalized = text.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "true" | "1" | "yes" => Some(true),
                "false" | "0" | "no" => Some(false),
                _ => None,
            }
        }
        Value::Array(values) => values.into_iter().next().and_then(value_to_bool_tolerant),
        Value::Null | Value::Object(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::HnItem;

    /// Guards against silently dropping the reserved JSON field `type`.
    #[test]
    fn item_deserialization_maps_type_field() {
        let raw = r#"{
            "id": 8863,
            "type": "story",
            "by": "dhouston",
            "title": "My YC app: Dropbox - Throw away your USB drive"
        }"#;

        let item: HnItem = serde_json::from_str(raw).expect("valid item JSON should deserialize");

        assert_eq!(item.id, 8863);
        assert_eq!(item.type_.as_deref(), Some("story"));
        assert_eq!(item.by.as_deref(), Some("dhouston"));
    }

    /// Guards against decode failures when boolean fields are unexpectedly returned as arrays.
    #[test]
    fn item_deserialization_tolerates_array_boolean_shape() {
        let raw = r#"{
            "id": 41550939,
            "deleted": [true],
            "dead": [],
            "type": "story"
        }"#;

        let item: HnItem =
            serde_json::from_str(raw).expect("array boolean shape should deserialize");

        assert_eq!(item.id, 41550939);
        assert_eq!(item.deleted, Some(true));
        assert_eq!(item.dead, None);
        assert_eq!(item.type_.as_deref(), Some("story"));
    }
}
