//! Host-side helpers for driving `TinyMCP` clients over `TinyBus`.
//!
//! This crate contains client lifecycle state and admission bookkeeping. It
//! depends on the contract crate for message types and does not link the MCP
//! module implementation. The public lifecycle helpers are available at
//! [`client_lifecycle`].
//!
//! A host can drive the lifecycle without performing bus I/O in this crate:
//!
//! ```
//! use tinymcp_client::{McpClientConfig, OpenConfiguredResponse, TransitionStatus};
//! use tinymcp_client::client_lifecycle::{ConfiguredLifecycle, Step};
//!
//! let mut lifecycle = ConfiguredLifecycle::new();
//! let config = McpClientConfig::default();
//! assert!(matches!(lifecycle.open_step(&config), Step::CallOpen(_)));
//! lifecycle.open_replied(Ok(OpenConfiguredResponse {
//!     object_path: "/objects/workspace".to_owned(),
//!     transition_id: "transition-1".to_owned(),
//! }));
//! assert_eq!(lifecycle.open_step(&config), Step::PollOpen("transition-1".to_owned()));
//! lifecycle.open_polled(TransitionStatus::Complete);
//! assert!(matches!(lifecycle.open_step(&config), Step::Ready(_)));
//! ```

pub mod client_lifecycle;

pub use tinymcp_bus::{McpClientConfig, OpenConfiguredResponse, TransitionStatus};
