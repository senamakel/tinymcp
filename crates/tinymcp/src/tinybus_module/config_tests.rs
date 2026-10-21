//! Configuration compatibility and storage descriptor wire shape.

use super::*;
use serde_json::json;

#[test]
fn old_empty_and_null_configurations_omit_the_storage_descriptor()
-> Result<(), Box<dyn std::error::Error>> {
    for value in [json!({}), serde_json::Value::Null] {
        let config: ModuleConfig = serde_json::from_value(value)?;
        assert!(config.storage_descriptor.is_none());
        let encoded = serde_json::to_value(config)?;

        assert!(encoded.get("storage_descriptor").is_none());
    }
    Ok(())
}

#[test]
fn a_host_storage_descriptor_round_trips_through_module_config()
-> Result<(), Box<dyn std::error::Error>> {
    let descriptor = json!({
        "service_peer": ":1.7",
        "registration_id": 42,
        "object_root": "/ai/tinyhumans/tinystorage/Leases/42"
    });
    let config: ModuleConfig =
        serde_json::from_value(json!({"storage_descriptor": descriptor.clone()}))?;
    let encoded = serde_json::to_value(config)?;

    assert_eq!(encoded["storage_descriptor"], descriptor);
    Ok(())
}
