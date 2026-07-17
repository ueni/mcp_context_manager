//! Native MCP transport and process lifecycle.

mod registry;

use std::{
    collections::HashMap,
    convert::Infallible,
    env,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{HeaderMap, Request, StatusCode, header},
    middleware::{self, Next},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use context_core::{
    ContextAdminRequest, ContextLookupRequest, ContextMemoryRequest, ContextPackRequest,
    ResultReferenceRequest, unloaded_admin_response,
};
use http_body_util::{BodyExt, Limited};
use registry::ProjectRegistry;
use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{
        Implementation, ListResourceTemplatesResult, ListResourcesResult, PaginatedRequestParams,
        ReadResourceRequestParams, ReadResourceResult, Resource, ResourceContents,
        ResourceTemplate, ServerCapabilities, ServerInfo,
    },
    service::RequestContext,
    tool, tool_handler, tool_router,
    transport::{
        stdio,
        streamable_http_server::{
            StreamableHttpServerConfig, StreamableHttpService,
            session::local::{LocalSessionManager, SessionConfig},
        },
    },
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt as TowerServiceExt;
use uuid::Uuid;

pub const SERVER_NAME: &str = "mcp-context-manager";
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
const LEGACY_SSE_MAX_SESSIONS: usize = 64;
const LEGACY_SSE_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const LEGACY_SSE_RESPONSE_LIMIT: usize = 4 * 1024 * 1024;

type NativeMcpService = StreamableHttpService<ContextServer, LocalSessionManager>;

#[derive(Clone)]
pub struct ContextServer {
    registry: Arc<ProjectRegistry>,
    tool_router: ToolRouter<Self>,
}

impl ContextServer {
    pub fn new(registry: Arc<ProjectRegistry>) -> Self {
        Self {
            registry,
            tool_router: Self::tool_router(),
        }
    }
}

#[derive(Clone)]
struct HttpState {
    registry: Arc<ProjectRegistry>,
    security: Arc<HttpSecurity>,
    legacy_sse: Arc<LegacySseBridge>,
}

struct LegacySseBridge {
    mcp: NativeMcpService,
    sessions: Mutex<HashMap<String, LegacySseSession>>,
}

struct LegacySseSession {
    sender: mpsc::Sender<String>,
    mcp_session_id: Option<String>,
    protocol_version: Option<String>,
    expires_at: Instant,
}

#[derive(Debug)]
struct HttpSecurity {
    bearer_token: Option<String>,
    allowed_hosts: Vec<String>,
    allowed_origins: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ReferenceQuery {
    project_id: Option<String>,
    root_uri: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LegacySseQuery {
    session_id: String,
}

#[tool_router]
impl ContextServer {
    #[tool(description = "Report native server readiness")]
    fn health(&self) -> String {
        "ok".to_owned()
    }

    #[tool(description = "Build a compact, cited repository context pack using context_pack.v2")]
    async fn context_pack(
        &self,
        Parameters(mut request): Parameters<ContextPackRequest>,
    ) -> Result<String, String> {
        let engine = self
            .registry
            .engine_for(request.project_id.as_deref(), request.root_uri.as_deref())
            .map_err(|error| error.to_string())?;
        request.project_id = Some(engine.project_id().to_owned());
        request.root_uri = None;
        let encoded = engine
            .context_pack_cached(&request)
            .await
            .map_err(|error| error.to_string())?;
        String::from_utf8(encoded).map_err(|error| error.to_string())
    }

    #[tool(description = "Search or read a targeted repository snippet")]
    fn context_lookup(
        &self,
        Parameters(mut request): Parameters<ContextLookupRequest>,
    ) -> Result<String, String> {
        let engine = self
            .registry
            .engine_for(request.project_id.as_deref(), request.root_uri.as_deref())
            .map_err(|error| error.to_string())?;
        request.project_id = Some(engine.project_id().to_owned());
        request.root_uri = None;
        let encoded = engine
            .context_lookup(&request)
            .map_err(|error| error.to_string())?;
        String::from_utf8(encoded).map_err(|error| error.to_string())
    }

    #[tool(description = "Manage compact repository-local context memory")]
    fn context_memory(
        &self,
        Parameters(mut request): Parameters<ContextMemoryRequest>,
    ) -> Result<String, String> {
        let engine = self
            .registry
            .engine_for(request.project_id.as_deref(), request.root_uri.as_deref())
            .map_err(|error| error.to_string())?;
        request.project_id = Some(engine.project_id().to_owned());
        request.root_uri = None;
        let encoded = engine
            .context_memory(&request)
            .map_err(|error| error.to_string())?;
        String::from_utf8(encoded).map_err(|error| error.to_string())
    }

    #[tool(
        description = "Resolve a local result reference after boundary, expiry, and hash checks"
    )]
    fn result_reference_resolve(
        &self,
        Parameters(mut request): Parameters<ResultReferenceRequest>,
    ) -> Result<String, String> {
        let engine = self
            .registry
            .engine_for(request.project_id.as_deref(), request.root_uri.as_deref())
            .map_err(|error| error.to_string())?;
        request.project_id = Some(engine.project_id().to_owned());
        request.root_uri = None;
        let encoded = engine
            .result_reference_resolve(&request)
            .map_err(|error| error.to_string())?;
        String::from_utf8(encoded).map_err(|error| error.to_string())
    }

    #[tool(
        description = "Inspect health, index, cache, contracts, metrics, quality, and generated state"
    )]
    fn context_admin(
        &self,
        Parameters(mut request): Parameters<ContextAdminRequest>,
    ) -> Result<String, String> {
        if request.mode == "projects" {
            return serde_json::to_string(
                &self
                    .registry
                    .projects_payload()
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string());
        }
        if request.mode == "active_projects" {
            return serde_json::to_string(
                &self
                    .registry
                    .active_projects_payload()
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string());
        }
        if request.mode == "cached_projects" {
            return serde_json::to_string(
                &self
                    .registry
                    .cached_projects_payload()
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string());
        }
        if is_read_only_admin_mode(&request.mode) {
            let (project_id, engine, known) = self
                .registry
                .cached_engine_for(request.project_id.as_deref(), request.root_uri.as_deref())
                .map_err(|error| error.to_string())?;
            request.project_id = Some(project_id);
            request.root_uri = None;
            let encoded = match engine {
                Some(engine) => engine.context_admin(&request),
                None => unloaded_admin_response(
                    &request,
                    request.project_id.as_deref().unwrap_or_default(),
                    if known { "unloaded" } else { "unknown_project" },
                ),
            }
            .map_err(|error| error.to_string())?;
            return String::from_utf8(encoded).map_err(|error| error.to_string());
        }
        let engine = self
            .registry
            .engine_for(request.project_id.as_deref(), request.root_uri.as_deref())
            .map_err(|error| error.to_string())?;
        request.project_id = Some(engine.project_id().to_owned());
        request.root_uri = None;
        let encoded = engine
            .context_admin(&request)
            .map_err(|error| error.to_string())?;
        String::from_utf8(encoded).map_err(|error| error.to_string())
    }
}

fn is_read_only_admin_mode(mode: &str) -> bool {
    matches!(
        mode,
        "metrics" | "measurement_matrix" | "metrics_and_matrix"
    )
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for ContextServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
        )
        .with_server_info(Implementation::new(SERVER_NAME, SERVER_VERSION))
        .with_instructions("Native context manager with clean-break context_pack.v2")
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        Ok(ListResourcesResult::with_all_items(vec![
            Resource::new("repo://summary", "Repository summary")
                .with_mime_type("application/json"),
            Resource::new("repo://tree/.", "Repository tree").with_mime_type("application/json"),
            Resource::new("repo://metrics", "Repository metrics")
                .with_mime_type("application/json"),
            Resource::new(
                "repo://instructions/codex-context-pack-first",
                "MCP-first instructions",
            )
            .with_mime_type("application/json"),
        ]))
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, McpError> {
        Ok(ListResourceTemplatesResult::with_all_items(vec![
            ResourceTemplate::new("repo://file/{path}", "Repository file")
                .with_mime_type("text/plain"),
            ResourceTemplate::new("repo://tree/{path}", "Repository tree")
                .with_mime_type("application/json"),
            ResourceTemplate::new("repo://context/{reference_id}", "Deferred context")
                .with_mime_type("application/json"),
            ResourceTemplate::new(
                "repo://project/{project_id}/file/{path}",
                "Project repository file",
            )
            .with_mime_type("text/plain"),
        ]))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        let text = self
            .registry
            .engine_for(project_id_from_resource(&request.uri), None)
            .map_err(|error| McpError::invalid_params(error.to_string(), None))?
            .resource_text(&request.uri)
            .map_err(|error| McpError::invalid_params(error.to_string(), None))?;
        Ok(ReadResourceResult::new(vec![ResourceContents::text(
            text,
            request.uri,
        )]))
    }
}

pub async fn run_from_env() -> Result<()> {
    let mut args = env::args().skip(1);
    let mut transport = env::var("MCP_TRANSPORT").unwrap_or_else(|_| "stdio".to_owned());

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--version" | "-V" => {
                println!("{SERVER_NAME} {SERVER_VERSION}");
                return Ok(());
            }
            "--transport" => {
                transport = args.next().context("--transport requires a value")?;
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    let root = env::var_os("REPO_PATH")
        .map(PathBuf::from)
        .map_or_else(env::current_dir, Ok)
        .context("resolve repository root")?;
    let mut state = env::var_os("MCP_CONTEXT_STATE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join(".mcp-context-manager"));
    if !state.is_absolute() {
        state = root.join(state);
    }
    let registry =
        Arc::new(ProjectRegistry::from_env(root, state).context("build native project registry")?);

    match transport.as_str() {
        "stdio" => run_stdio(registry).await,
        "streamable-http" | "http" => run_http(registry).await,
        other => bail!("unsupported MCP transport: {other}"),
    }
}

async fn run_stdio(registry: Arc<ProjectRegistry>) -> Result<()> {
    let service = ContextServer::new(registry).serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}

async fn run_http(registry: Arc<ProjectRegistry>) -> Result<()> {
    let port = env::var("PORT")
        .unwrap_or_else(|_| "8000".to_owned())
        .parse::<u16>()
        .context("PORT must be a valid TCP port")?;
    let host = env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let address: SocketAddr = format!("{host}:{port}")
        .parse()
        .context("HOST and PORT must form a valid socket address")?;
    let security = Arc::new(HttpSecurity::from_env(&host, port));

    let cancellation = CancellationToken::new();
    let mut local_sessions = LocalSessionManager::default();
    let mut session_config = SessionConfig::default();
    session_config.keep_alive = Some(Duration::from_secs(30 * 60));
    local_sessions.session_config = session_config;
    let session_manager = Arc::new(local_sessions);
    let mcp_config = StreamableHttpServerConfig::default()
        .with_json_response(true)
        .with_sse_keep_alive(None)
        .with_allowed_hosts(security.allowed_hosts.clone())
        .with_allowed_origins(security.allowed_origins.clone())
        .with_cancellation_token(cancellation.child_token());
    let mcp_registry = Arc::clone(&registry);
    let mcp: NativeMcpService = StreamableHttpService::new(
        move || Ok(ContextServer::new(Arc::clone(&mcp_registry))),
        session_manager,
        mcp_config,
    );
    let state = HttpState {
        registry: Arc::clone(&registry),
        security: Arc::clone(&security),
        legacy_sse: Arc::new(LegacySseBridge::new(mcp.clone())),
    };
    let app = Router::new()
        .route("/", get(root))
        .route("/healthz", get(healthz))
        .route("/mcp/healthz", get(healthz))
        .route("/legacy/sse", get(legacy_sse_http))
        .route("/legacy/messages", post(legacy_sse_message_http))
        .route("/v1/mcp/tools", get(mcp_tools_http))
        .route("/v1/context/pack", post(context_pack_http))
        .route("/v1/context/references/{reference_id}", get(reference_http))
        .nest_service("/mcp", mcp)
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(
            state.security,
            validate_http_request,
        ));
    let listener = tokio::net::TcpListener::bind(address).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(cancellation))
        .await?;
    Ok(())
}

async fn root() -> &'static str {
    SERVER_NAME
}

async fn healthz() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "ok": true,
        "server": SERVER_NAME,
        "version": SERVER_VERSION,
        "native": true
    }))
}

async fn mcp_tools_http() -> Json<Value> {
    let tools = [
        "context_pack",
        "context_lookup",
        "context_memory",
        "context_admin",
        "result_reference_resolve",
    ];
    Json(json!({
        "schema": "context_http.mcp_tools.v1",
        "mcp_endpoint": "/mcp",
        "legacy_sse_endpoint": "/legacy/sse",
        "tool_count": tools.len(),
        "tools": tools,
    }))
}

impl LegacySseBridge {
    fn new(mcp: NativeMcpService) -> Self {
        Self {
            mcp,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    fn open_session(&self) -> Result<(String, mpsc::Receiver<String>), &'static str> {
        let mut sessions = self.lock_sessions();
        let now = Instant::now();
        sessions.retain(|_, session| session.expires_at > now);
        if sessions.len() >= LEGACY_SSE_MAX_SESSIONS {
            return Err("legacy SSE session capacity has been reached");
        }
        let session_id = Uuid::new_v4().to_string();
        let (sender, receiver) = mpsc::channel(32);
        sessions.insert(
            session_id.clone(),
            LegacySseSession {
                sender,
                mcp_session_id: None,
                protocol_version: None,
                expires_at: now + LEGACY_SSE_IDLE_TIMEOUT,
            },
        );
        Ok((session_id, receiver))
    }

    fn session_for_message(&self, session_id: &str) -> Option<(Option<String>, Option<String>)> {
        let mut sessions = self.lock_sessions();
        let now = Instant::now();
        let session = sessions.get_mut(session_id)?;
        if session.expires_at <= now {
            sessions.remove(session_id);
            return None;
        }
        session.expires_at = now + LEGACY_SSE_IDLE_TIMEOUT;
        Some((
            session.mcp_session_id.clone(),
            session.protocol_version.clone(),
        ))
    }

    fn set_mcp_session(
        &self,
        session_id: &str,
        mcp_session_id: Option<String>,
        protocol_version: Option<String>,
    ) {
        let mut sessions = self.lock_sessions();
        let Some(session) = sessions.get_mut(session_id) else {
            return;
        };
        if let Some(mcp_session_id) = mcp_session_id {
            session.mcp_session_id = Some(mcp_session_id);
        }
        if let Some(protocol_version) = protocol_version {
            session.protocol_version = Some(protocol_version);
        }
        session.expires_at = Instant::now() + LEGACY_SSE_IDLE_TIMEOUT;
    }

    fn send_messages(&self, session_id: &str, messages: Vec<String>) -> Result<(), &'static str> {
        let sender = {
            let mut sessions = self.lock_sessions();
            let now = Instant::now();
            let session = sessions
                .get_mut(session_id)
                .ok_or("legacy SSE session was not found")?;
            if session.expires_at <= now {
                sessions.remove(session_id);
                return Err("legacy SSE session has expired");
            }
            session.expires_at = now + LEGACY_SSE_IDLE_TIMEOUT;
            session.sender.clone()
        };
        for message in messages {
            sender
                .try_send(message)
                .map_err(|_| "legacy SSE client is not accepting messages")?;
        }
        Ok(())
    }

    fn lock_sessions(&self) -> std::sync::MutexGuard<'_, HashMap<String, LegacySseSession>> {
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

async fn legacy_sse_http(State(state): State<HttpState>, headers: HeaderMap) -> Response {
    let (session_id, receiver) = match state.legacy_sse.open_session() {
        Ok(session) => session,
        Err(message) => return rest_error(StatusCode::SERVICE_UNAVAILABLE, message),
    };
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("localhost");
    let endpoint = format!("http://{host}/legacy/messages?session_id={session_id}");
    let events =
        tokio_stream::iter([Ok::<Event, Infallible>(
            Event::default().event("endpoint").data(endpoint),
        )])
        .chain(ReceiverStream::new(receiver).map(|message| {
            Ok::<Event, Infallible>(Event::default().event("message").data(message))
        }));
    Sse::new(events)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keepalive"),
        )
        .into_response()
}

async fn legacy_sse_message_http(
    State(state): State<HttpState>,
    Query(query): Query<LegacySseQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (mcp_session_id, saved_protocol_version) =
        match state.legacy_sse.session_for_message(&query.session_id) {
            Some(session) => session,
            None => return rest_error(StatusCode::NOT_FOUND, "legacy SSE session was not found"),
        };
    let request_protocol_version =
        serde_json::from_slice::<Value>(&body)
            .ok()
            .and_then(|message| {
                message
                    .pointer("/params/protocolVersion")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            });

    let mut request = Request::builder()
        .method("POST")
        .uri("/")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(host) = headers.get(header::HOST) {
        request = request.header(header::HOST, host);
    }
    if let Some(origin) = headers.get(header::ORIGIN) {
        request = request.header(header::ORIGIN, origin);
    }
    if let Some(mcp_session_id) = mcp_session_id.as_deref() {
        request = request.header("mcp-session-id", mcp_session_id);
    }
    if let Some(protocol_version) = saved_protocol_version
        .as_deref()
        .or_else(|| request_protocol_version.as_deref())
        .or_else(|| {
            headers
                .get("mcp-protocol-version")
                .and_then(|value| value.to_str().ok())
        })
    {
        request = request.header("mcp-protocol-version", protocol_version);
    }
    let request = request
        .body(Body::from(body))
        .expect("legacy SSE proxy request is valid");
    let response = state
        .legacy_sse
        .mcp
        .clone()
        .oneshot(request)
        .await
        .expect("streamable MCP service is infallible");
    let status = response.status();
    let content_type = response.headers().get(header::CONTENT_TYPE).cloned();
    let mcp_response_session = response
        .headers()
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let bytes = match Limited::new(response.into_body(), LEGACY_SSE_RESPONSE_LIMIT)
        .collect()
        .await
    {
        Ok(collected) => collected.to_bytes(),
        Err(_) => {
            return rest_error(
                StatusCode::BAD_GATEWAY,
                "legacy SSE proxy response exceeded its size limit",
            );
        }
    };
    if !status.is_success() {
        let mut response = Response::builder().status(status);
        if let Some(content_type) = content_type {
            response = response.header(header::CONTENT_TYPE, content_type);
        }
        return response
            .body(Body::from(bytes))
            .expect("legacy SSE proxy error response is valid");
    }

    let messages = match legacy_sse_messages(&bytes, content_type.as_ref()) {
        Ok(messages) => messages,
        Err(message) => return rest_error(StatusCode::BAD_GATEWAY, message),
    };
    state.legacy_sse.set_mcp_session(
        &query.session_id,
        mcp_response_session,
        request_protocol_version,
    );
    if let Err(message) = state.legacy_sse.send_messages(&query.session_id, messages) {
        return rest_error(StatusCode::SERVICE_UNAVAILABLE, message);
    }
    StatusCode::ACCEPTED.into_response()
}

fn legacy_sse_messages(
    bytes: &[u8],
    content_type: Option<&axum::http::HeaderValue>,
) -> Result<Vec<String>, &'static str> {
    let text = std::str::from_utf8(bytes).map_err(|_| "MCP response was not valid UTF-8")?;
    if content_type.is_some_and(|value| {
        value
            .to_str()
            .is_ok_and(|value| value.starts_with("application/json"))
    }) {
        return Ok((!text.is_empty())
            .then(|| text.to_owned())
            .into_iter()
            .collect());
    }

    let normalized = text.replace("\r\n", "\n");
    let mut messages = Vec::new();
    for event in normalized.split("\n\n") {
        let mut name = None;
        let mut data = Vec::new();
        for line in event.lines() {
            if let Some(value) = line.strip_prefix("event:") {
                name = Some(value.trim());
            } else if let Some(value) = line.strip_prefix("data:") {
                data.push(value.strip_prefix(' ').unwrap_or(value));
            }
        }
        if matches!(name, None | Some("message")) && !data.is_empty() {
            messages.push(data.join("\n"));
        }
    }
    if normalized.trim().is_empty() || !messages.is_empty() {
        Ok(messages)
    } else {
        Err("MCP response did not contain a JSON-RPC SSE message")
    }
}

async fn context_pack_http(
    State(state): State<HttpState>,
    Json(mut payload): Json<Value>,
) -> Response {
    let Some(object) = payload.as_object_mut() else {
        return rest_error(StatusCode::BAD_REQUEST, "JSON body must be an object");
    };
    if !object.contains_key("prompt")
        && let Some(task) = object.remove("task")
    {
        object.insert("prompt".to_owned(), task);
    }
    let mut request = match serde_json::from_value::<ContextPackRequest>(payload) {
        Ok(request) if !request.prompt.trim().is_empty() => request,
        Ok(_) => return rest_error(StatusCode::BAD_REQUEST, "prompt is required"),
        Err(error) => return rest_error(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    let engine = match state
        .registry
        .engine_for(request.project_id.as_deref(), request.root_uri.as_deref())
    {
        Ok(engine) => engine,
        Err(error) => return rest_error(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    request.project_id = Some(engine.project_id().to_owned());
    request.root_uri = None;
    match engine.context_pack_cached(&request).await {
        Ok(bytes) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(bytes))
            .expect("valid REST response"),
        Err(error) => rest_error(StatusCode::BAD_REQUEST, &error.to_string()),
    }
}

async fn reference_http(
    State(state): State<HttpState>,
    Path(reference_id): Path<String>,
    Query(query): Query<ReferenceQuery>,
) -> Response {
    let engine = match state
        .registry
        .engine_for(query.project_id.as_deref(), query.root_uri.as_deref())
    {
        Ok(engine) => engine,
        Err(error) => return rest_error(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    let request = ResultReferenceRequest {
        reference_id,
        reference: None,
        expected_hash: String::new(),
        project_id: Some(engine.project_id().to_owned()),
        root_uri: None,
    };
    match engine.result_reference_resolve(&request) {
        Ok(bytes) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(bytes))
            .expect("valid reference response"),
        Err(error) => rest_error(StatusCode::BAD_REQUEST, &error.to_string()),
    }
}

async fn validate_http_request(
    State(security): State<Arc<HttpSecurity>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok());
    if !host.is_some_and(|host| {
        security
            .allowed_hosts
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(host))
    }) {
        return rest_error(StatusCode::FORBIDDEN, "Host is not allowed");
    }
    if let Some(origin) = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        && !security
            .allowed_origins
            .iter()
            .any(|allowed| allowed == origin)
    {
        return rest_error(StatusCode::FORBIDDEN, "Origin is not allowed");
    }
    if let Some(expected) = security.bearer_token.as_deref() {
        let provided = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .unwrap_or_default();
        if !constant_time_eq(expected.as_bytes(), provided.as_bytes()) {
            return Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header(header::WWW_AUTHENTICATE, "Bearer")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "schema": "context_http.error.v1",
                        "error": "unauthorized",
                        "message": "valid bearer authentication is required",
                    })
                    .to_string(),
                ))
                .expect("valid unauthorized response");
        }
    }
    next.run(request).await
}

fn rest_error(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(json!({
            "schema": "context_http.error.v1",
            "error": if status == StatusCode::BAD_REQUEST {"bad_request"} else {"request_rejected"},
            "message": message,
        })),
    )
        .into_response()
}

impl HttpSecurity {
    fn from_env(host: &str, port: u16) -> Self {
        let configured_hosts = split_csv_env("MCP_HTTP_ALLOWED_HOSTS");
        let allowed_hosts = if configured_hosts.is_empty() {
            let mut hosts = vec![
                "localhost".to_owned(),
                format!("localhost:{port}"),
                "127.0.0.1".to_owned(),
                format!("127.0.0.1:{port}"),
                "[::1]".to_owned(),
                format!("[::1]:{port}"),
            ];
            if host != "0.0.0.0" && host != "::" {
                hosts.push(host.to_owned());
                hosts.push(format!("{host}:{port}"));
            }
            hosts
        } else {
            configured_hosts
        };
        let configured_origins = split_csv_env("MCP_HTTP_ALLOWED_ORIGINS");
        let allowed_origins = if configured_origins.is_empty() {
            vec![
                format!("http://localhost:{port}"),
                format!("http://127.0.0.1:{port}"),
            ]
        } else {
            configured_origins
        };
        Self {
            bearer_token: env::var("MCP_HTTP_BEARER_TOKEN")
                .ok()
                .filter(|token| !token.trim().is_empty()),
            allowed_hosts,
            allowed_origins,
        }
    }
}

fn split_csv_env(name: &str) -> Vec<String> {
    env::var(name)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

fn constant_time_eq(expected: &[u8], provided: &[u8]) -> bool {
    let length = expected.len().max(provided.len());
    let mut difference = expected.len() ^ provided.len();
    for index in 0..length {
        difference |= usize::from(expected.get(index).copied().unwrap_or_default())
            ^ usize::from(provided.get(index).copied().unwrap_or_default());
    }
    difference == 0
}

async fn shutdown_signal(cancellation: CancellationToken) {
    let _ = tokio::signal::ctrl_c().await;
    cancellation.cancel();
}

fn project_id_from_resource(uri: &str) -> Option<&str> {
    uri.strip_prefix("repo://project/")
        .and_then(|rest| rest.split_once('/'))
        .map(|(project_id, _)| project_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_admin_returns_an_unloaded_snapshot_without_constructing_an_engine() {
        let root = tempfile::tempdir().expect("repository root");
        let state = tempfile::tempdir().expect("state root");
        std::fs::write(root.path().join("Cargo.toml"), "[workspace]\n").expect("project marker");
        let registry = Arc::new(
            ProjectRegistry::new(
                root.path().to_owned(),
                state.path().to_owned(),
                Vec::new(),
                Vec::new(),
            )
            .expect("registry"),
        );
        let server = ContextServer::new(registry);
        let active_request: ContextAdminRequest =
            serde_json::from_value(json!({"mode": "active_projects"}))
                .expect("active projects request");
        let active: Value = serde_json::from_str(
            &server
                .context_admin(Parameters(active_request))
                .expect("active projects response"),
        )
        .expect("active projects JSON");
        assert_eq!(active["schema"], "context_projects.active.v1");
        assert_eq!(active["count"], 0);
        let cached_request: ContextAdminRequest =
            serde_json::from_value(json!({"mode": "cached_projects"}))
                .expect("cached projects request");
        let cached: Value = serde_json::from_str(
            &server
                .context_admin(Parameters(cached_request))
                .expect("cached projects response"),
        )
        .expect("cached projects JSON");
        assert_eq!(cached["schema"], "context_projects.cached.v1");
        let request: ContextAdminRequest =
            serde_json::from_value(json!({"mode": "metrics_and_matrix"})).expect("metrics request");

        let response: Value = serde_json::from_str(
            &server
                .context_admin(Parameters(request))
                .expect("metrics response"),
        )
        .expect("metrics JSON");
        assert_eq!(response["metrics"]["background"]["status"], "unloaded");
    }

    #[tokio::test]
    async fn mcp_tools_contract_advertises_legacy_sse_compatibility_endpoint() {
        let Json(payload) = mcp_tools_http().await;

        assert_eq!(payload["mcp_endpoint"], "/mcp");
        assert_eq!(payload["tool_count"], 5);
        assert_eq!(
            payload["tools"],
            json!([
                "context_pack",
                "context_lookup",
                "context_memory",
                "context_admin",
                "result_reference_resolve",
            ])
        );
        assert_eq!(payload["legacy_sse_endpoint"], "/legacy/sse");
    }

    #[test]
    fn legacy_sse_parser_ignores_priming_events_and_preserves_json_rpc() {
        let messages = legacy_sse_messages(
            b"retry: 3000\n\n\
              event: message\n\
              data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}\n\n",
            None,
        )
        .expect("valid RMCP SSE response");

        assert_eq!(
            messages,
            vec!["{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}"]
        );
    }
}
