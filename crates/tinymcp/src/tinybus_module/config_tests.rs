//! Configuration compatibility and storage descriptor wire shape.

use super::*;
use serde_json::json;

#[test]
fn old_empty_and_null_configurations_omit_the_storage_descriptor() {
    for value in [json!({}), serde_json::Value::Null] {
        let config: ModuleConfig = serde_json::from_value(value).unwrap();
        let encoded = serde_json::to_value(config).unwrap();

        assert!(encoded.get("storage_descriptor").is_none());
    }
}

#[test]
fn a_host_storage_descriptor_round_trips_through_module_config() {
    let descriptor = json!({
        "service_peer": ":1.7",
        "registration_id": 42,
        "object_root": "/ai/tinyhumans/tinystorage/Leases/42"
    });
    let config: ModuleConfig =
        serde_json::from_value(json!({"storage_descriptor": descriptor.clone()})).unwrap();
    let encoded = serde_json::to_value(config).unwrap();

    assert_eq!(encoded["storage_descriptor"], descriptor);
}
