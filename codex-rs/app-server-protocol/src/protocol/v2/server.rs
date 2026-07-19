use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use ts_rs::TS;

/// Read-only diagnostic inventory of initialized app-server connections.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ServerConnectionListParams {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ServerConnection {
    /// Process-local opaque connection id. It is not a credential and changes after reconnect.
    pub id: String,
    /// Initialized `clientInfo.name`; no request payloads or authorization material are exposed.
    pub client_name: Option<String>,
    pub request_attestation: bool,
    pub is_current: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ServerConnectionListResponse {
    pub data: Vec<ServerConnection>,
}
