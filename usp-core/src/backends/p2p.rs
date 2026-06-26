//! P2P storage backend using libp2p Kademlia DHT
//!
//! This backend provides:
//! - Content-addressed storage using DHT (Kademlia records + provider records)
//! - Peer-to-peer data retrieval
//! - Automatic content routing and discovery
//!
//! Architecture
//! ------------
//! A background tokio task drives a `libp2p::Swarm`. The public API sends
//! commands through an unbounded mpsc channel, and the swarm event loop
//! applies them (e.g. listening, dialing, storing/retrieving records).
//! Out-of-band query results are routed back to the requester via
//! per-request one-shot channels identified by a Kademlia `QueryId`.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use libp2p::{
    identify, identity as lp_identity, kad, multiaddr::Protocol, noise,
    request_response::{self, RequestResponseCodec},
    swarm::SwarmEvent, tcp, yamux, Multiaddr, PeerId, Swarm, Transport,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, RwLock};

use crate::backends::StorageBackend;
use crate::error::{Error, Result};
use crate::types::*;

/// Internal command sent to the swarm task. The reply sender carries the
/// *raw* response (already a `Result` where applicable) so the swarm task
/// can stream back per-step results from Kademlia events.
enum Command {
    /// Start listening on a multiaddr (typically `/ip4/0.0.0.0/tcp/0`).
    Listen {
        addr: String,
        reply: oneshot::Sender<std::result::Result<(), Error>>,
    },
    /// Dial + bootstrap a list of peer multiaddrs.
    Bootstrap {
        peers: Vec<String>,
        reply: oneshot::Sender<std::result::Result<(), Error>>,
    },
    /// Store a record in the DHT (value-bound, 24h TTL by default).
    PutRecord {
        key: String,
        value: Bytes,
        reply: oneshot::Sender<std::result::Result<(), Error>>,
    },
    /// Retrieve a record from the DHT (local first, then network).
    GetRecord {
        key: String,
        reply: oneshot::Sender<std::result::Result<Option<Bytes>, Error>>,
    },
    /// Announce that we are a provider for the given CID key.
    StartProviding {
        key: String,
        reply: oneshot::Sender<std::result::Result<(), Error>>,
    },
    /// Look for providers of a key on the DHT.
    GetProviders {
        key: String,
        reply: oneshot::Sender<std::result::Result<Vec<PeerId>, Error>>,
    },
    /// Get the local peer id.
    LocalPeerId { reply: oneshot::Sender<PeerId> },
    /// Request data by key from a specific peer.
    FetchFromPeer {
        peer: PeerId,
        key: String,
        reply: oneshot::Sender<std::result::Result<Option<Bytes>, Error>>,
    },
    /// Register a key+value pair as available for serving to other peers.
    RegisterServe {
        key: String,
        value: Bytes,
    },
    /// Disconnect gracefully.
    Shutdown { reply: oneshot::Sender<()> },
}

/// P2P storage backend backed by a libp2p Kademlia DHT.
pub struct P2PBackend {
    cmd_tx: mpsc::UnboundedSender<Command>,
    connected_peers: Arc<RwLock<Vec<PeerId>>>,
    stored_data: Arc<RwLock<HashMap<String, Bytes>>>,
    is_connected: Arc<RwLock<bool>>,
}

/// Combined network behaviour: Kademlia (DHT) + Identify (protocol negotiation).
mod behaviour {
    use libp2p::swarm::NetworkBehaviour;
    use libp2p::{identify, kad, request_response};

    #[derive(NetworkBehaviour)]
    #[behaviour(prelude = "libp2p::swarm::derive_prelude")]
    pub struct MyBehaviour {
        pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
        pub identify: identify::Behaviour,
        pub fetch: request_response::Behaviour<super::FetchCodec>,
    }
}

/// Simple length-prefixed codec for the fetch protocol.
/// Format: [4-byte BE length][key bytes] -> request
///         [4-byte BE length][data bytes] -> response
#[derive(Clone, Default)]
pub struct FetchCodec;

#[async_trait]
impl RequestResponseCodec for FetchCodec {
    type Protocol = Vec<u8>;
    type Request = Vec<u8>;
    type Response = Vec<u8>;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> std::io::Result<Self::Request>
    where
        T: futures::AsyncRead + Unpin + Send,
    {
        let len = read_u32_be(io).await?;
        let mut buf = vec![0u8; len as usize];
        io.read_exact(&mut buf).await?;
        Ok(buf)
    }

    async fn read_response<T>(&mut self, _: &Self::Protocol, io: &mut T) -> std::io::Result<Self::Response>
    where
        T: futures::AsyncRead + Unpin + Send,
    {
        let len = read_u32_be(io).await?;
        let mut buf = vec![0u8; len as usize];
        io.read_exact(&mut buf).await?;
        Ok(buf)
    }

    async fn write_request<T>(&mut self, _: &Self::Protocol, io: &mut T, req: Self::Request) -> std::io::Result<()>
    where
        T: futures::AsyncWrite + Unpin + Send,
    {
        write_u32_be(io, req.len() as u32).await?;
        io.write_all(&req).await?;
        io.close().await?;
        Ok(())
    }

    async fn write_response<T>(&mut self, _: &Self::Protocol, io: &mut T, resp: Self::Response) -> std::io::Result<()>
    where
        T: futures::AsyncWrite + Unpin + Send,
    {
        write_u32_be(io, resp.len() as u32).await?;
        io.write_all(&resp).await?;
        io.close().await?;
        Ok(())
    }
}

async fn read_u32_be<T: futures::AsyncRead + Unpin>(io: &mut T) -> std::io::Result<u32> {
    let mut buf = [0u8; 4];
    io.read_exact(&mut buf).await?;
    Ok(u32::from_be_bytes(buf))
}

async fn write_u32_be<T: futures::AsyncWrite + Unpin>(io: &mut T, val: u32) -> std::io::Result<()> {
    io.write_all(&val.to_be_bytes()).await
}

use behaviour::MyBehaviour;
use behaviour::MyBehaviourEvent;

impl P2PBackend {
    /// Create a new P2P backend. Spawns a background swarm task.
    pub fn new() -> Result<Self> {
        let keypair = lp_identity::Keypair::generate_ed25519();

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<Command>();

        let stored_data = Arc::new(RwLock::new(HashMap::new()));
        let connected_peers = Arc::new(RwLock::new(Vec::new()));
        let is_connected = Arc::new(RwLock::new(false));

        // Spawn the swarm task.
        tokio::spawn(swarm_task(
            keypair,
            cmd_rx,
            connected_peers.clone(),
            is_connected.clone(),
        ));

        Ok(Self {
            cmd_tx,
            connected_peers,
            stored_data,
            is_connected,
        })
    }

    /// Get the local peer id (read from the swarm task once it has booted).
    pub async fn peer_id(&self) -> Option<PeerId> {
        let (tx, rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Command::LocalPeerId { reply: tx })
            .is_err()
        {
            return None;
        }
        rx.await.ok()
    }

    /// Get list of currently connected peers (snapshot from the swarm).
    pub async fn connected_peers(&self) -> Vec<PeerId> {
        self.connected_peers.read().await.clone()
    }

    /// Bootstrap to known bootstrap nodes. Each multiaddr is parsed and dialed;
    /// once at least one peer is reachable Kademlia will start forming buckets.
    pub async fn bootstrap(&self, bootstrap_nodes: Vec<String>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Bootstrap {
                peers: bootstrap_nodes,
                reply: tx,
            })
            .map_err(|_| Error::Storage("P2P swarm task is not running".into()))?;
        rx.await
            .map_err(|_| Error::Storage("P2P swarm task dropped reply".into()))?
    }
}

impl Default for P2PBackend {
    fn default() -> Self {
        Self::new().expect("P2P keypair generation always succeeds")
    }
}

/// Lower-level helper: send a closure-built command and await the oneshot.
/// The reply channel carries the raw inner result; this helper wraps any
/// `Error` reply from the swarm into the outer `Result<T>`.
async fn send_cmd<T, F>(cmd_tx: &mpsc::UnboundedSender<Command>, f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(oneshot::Sender<std::result::Result<T, Error>>) -> Command,
{
    let (tx, rx) = oneshot::channel();
    cmd_tx
        .send(f(tx))
        .map_err(|_| Error::Storage("P2P swarm task is not running".into()))?;
    match rx.await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(Error::Storage("P2P swarm task dropped reply".into())),
    }
}

#[async_trait]
impl StorageBackend for P2PBackend {
    fn backend_type(&self) -> BackendType {
        BackendType::P2P
    }

    async fn init(&self, _config: crate::backends::BackendConfig) -> Result<()> {
        // Default listen address: ephemeral TCP port on all interfaces.
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Listen {
                addr: "/ip4/0.0.0.0/tcp/0".to_string(),
                reply: tx,
            })
            .map_err(|_| Error::Storage("P2P swarm task is not running".into()))?;
        rx.await
            .map_err(|_| Error::Storage("P2P swarm task dropped reply".into()))??;

        *self.is_connected.write().await = true;

        if let Some(pid) = self.peer_id().await {
            tracing::info!("P2P backend initialized with peer_id: {}", pid);
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(Command::Shutdown { reply: tx });
        let _ = rx.await;
        *self.connected_peers.write().await = Vec::new();
        *self.is_connected.write().await = false;
        Ok(())
    }

    async fn put(&self, key: &str, value: Bytes) -> Result<StoreReceipt> {
        let cid = crate::utils::cid::compute_cid(&value);

        // Always store locally for retrieval-by-key.
        {
            let mut stored = self.stored_data.write().await;
            stored.insert(key.to_string(), value.clone());
            stored.insert(cid.clone(), value.clone());
        }

        // Register data as available for serving to other peers
        let _ = self.cmd_tx.send(Command::RegisterServe {
            key: key.to_string(),
            value: value.clone(),
        });
        let _ = self.cmd_tx.send(Command::RegisterServe {
            key: cid.clone(),
            value,
        });

        // 1) Store the value as a DHT record under `key`.
        if let Err(e) = send_cmd(&self.cmd_tx, |reply| Command::PutRecord {
            key: key.to_string(),
            value: value.clone(),
            reply,
        })
        .await
        {
            tracing::warn!("P2P PutRecord failed for {}: {}", key, e);
        }

        // 2) Announce ourselves as a provider under the CID.
        if let Err(e) = send_cmd(&self.cmd_tx, |reply| Command::StartProviding {
            key: cid.clone(),
            reply,
        })
        .await
        {
            tracing::warn!("P2P StartProviding failed for {}: {}", cid, e);
        }

        Ok(StoreReceipt {
            content_hash: cid,
            backend: BackendType::P2P,
            stored_at: chrono::Utc::now(),
            size_bytes: value.len() as u64,
            pinned: true,
        })
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        // 1) Local cache first.
        if let Some(data) = self.stored_data.read().await.get(key).cloned() {
            return Ok(Some(data));
        }

        // 2) Try the DHT for a record under this key.
        let res: Result<Option<Bytes>> = send_cmd(&self.cmd_tx, |reply| Command::GetRecord {
            key: key.to_string(),
            reply,
        })
        .await;

        match res {
            Ok(Some(data)) => {
                self.stored_data
                    .write()
                    .await
                    .insert(key.to_string(), data.clone());
                return Ok(Some(data));
            }
            Ok(None) => {}
            Err(e) => {
                tracing::debug!("P2P GetRecord failed for {}: {}", key, e);
            }
        }

        // 3) Fall back: try the DHT for providers of this key.
        let providers: Result<Vec<PeerId>> =
            send_cmd(&self.cmd_tx, |reply| Command::GetProviders {
                key: key.to_string(),
                reply,
            })
            .await;

        if let Ok(providers) = providers {
            for provider in &providers {
                if *provider == self.peer_id().await.unwrap_or(*provider) {
                    continue; // Skip self
                }
                tracing::debug!("P2P fetching {} from provider {}", key, provider);
                let result: Result<Option<Bytes>> =
                    send_cmd(&self.cmd_tx, |reply| Command::FetchFromPeer {
                        peer: *provider,
                        key: key.to_string(),
                        reply,
                    })
                    .await;

                match result {
                    Ok(Some(data)) => {
                        self.stored_data
                            .write()
                            .await
                            .insert(key.to_string(), data.clone());
                        return Ok(Some(data));
                    }
                    Ok(None) => continue,
                    Err(e) => {
                        tracing::debug!("P2P fetch from {} failed: {}, trying next", provider, e);
                        continue;
                    }
                }
            }
        } else if let Err(e) = providers {
            tracing::debug!("P2P GetProviders failed for {}: {}", key, e);
        }

        Ok(None)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.stored_data.write().await.remove(key);
        // libp2p 0.53 MemoryStore does not expose record removal or
        // `stop_providing`; we rely on TTL expiry (24h) to clear DHT state.
        tracing::debug!("P2P delete: {} (DHT entries will expire by TTL)", key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.stored_data.read().await.contains_key(key))
    }

    async fn stats(&self) -> Result<BackendStats> {
        let stored = self.stored_data.read().await;
        let item_count = stored.len() as u64;
        let used_space: u64 = stored.values().map(|v| v.len() as u64).sum();
        let peer_count = self.connected_peers.read().await.len() as u32;
        Ok(BackendStats {
            total_capacity: u64::MAX, // P2P has no fixed capacity
            used_space,
            available_space: u64::MAX,
            item_count,
            peer_count,
        })
    }

    async fn list_keys(&self) -> Result<Vec<String>> {
        let stored = self.stored_data.read().await;
        // Only return user-facing keys (not CID keys).
        // Use is_valid_cid to detect CID keys (both v0 and v1).
        let keys: Vec<String> = stored
            .keys()
            .filter(|k| !crate::utils::cid::is_valid_cid(k))
            .cloned()
            .collect();
        Ok(keys)
    }
}

/// The background swarm task. Owns the `Swarm` and processes commands.
async fn swarm_task(
    keypair: lp_identity::Keypair,
    mut cmd_rx: mpsc::UnboundedReceiver<Command>,
    connected_peers: Arc<RwLock<Vec<PeerId>>>,
    _is_connected: Arc<RwLock<bool>>,
) {
    // Build the transport: TCP + Noise + Yamux.
    let transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true))
        .upgrade(libp2p::core::upgrade::Version::V1)
        .authenticate(noise::Config::new(&keypair).expect("signing libp2p-noise config"))
        .multiplex(yamux::Config::default())
        .boxed();

    // Behaviour: Kademlia + Identify.
    let local_peer_id = PeerId::from_public_key(&keypair.public());
    let store = kad::store::MemoryStore::new(local_peer_id);
    let kademlia = kad::Behaviour::new(local_peer_id, store);
    let identify_behaviour = identify::Behaviour::new(identify::Config::new(
        "/usp-kubo/id/1.0.0".to_string(),
        keypair.public(),
    ));

    let behaviour = MyBehaviour {
        kademlia,
        identify: identify_behaviour,
        fetch: request_response::Behaviour::new(
            vec![(b"/usp-kubo/fetch/1.0.0".to_vec(), request_response::ProtocolSupport::Full)],
            FetchCodec::default(),
            Duration::from_secs(30),
        ),
    };

    // Build the swarm with the legacy `Swarm::new` constructor + tokio executor.
    let config = libp2p::swarm::Config::with_tokio_executor();
    let mut swarm = Swarm::new(transport, behaviour, local_peer_id, config);

    tracing::info!("P2P swarm task started, local peer id: {}", local_peer_id);

    // Map of in-flight Kademlia query id -> oneshot reply sender.
    let mut pending_get_record: HashMap<kad::QueryId, oneshot::Sender<Result<Option<Bytes>>>> =
        HashMap::new();
    let mut pending_get_providers: HashMap<kad::QueryId, oneshot::Sender<Result<Vec<PeerId>>>> =
        HashMap::new();
    let mut pending_put_record: HashMap<kad::QueryId, oneshot::Sender<Result<()>>> = HashMap::new();
    let mut pending_start_providing: HashMap<kad::QueryId, oneshot::Sender<Result<()>>> =
        HashMap::new();
    // Data available for serving to other peers
    let mut serve_map: HashMap<String, Bytes> = HashMap::new();
    // Map of pending fetch requests (request_id -> reply sender)
    let mut pending_fetches: HashMap<request_response::RequestId, oneshot::Sender<std::result::Result<Option<Bytes>, Error>>> =
        HashMap::new();

    loop {
        tokio::select! {
            biased;

            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else {
                    tracing::info!("P2P swarm task: command channel closed, exiting");
                    return;
                };
                match cmd {
                    Command::Listen { addr, reply } => {
                        let res = match addr.parse::<Multiaddr>() {
                            Ok(ma) => match swarm.listen_on(ma) {
                                Ok(_) => Ok(()),
                                Err(e) => Err(Error::Network(format!("listen_on: {}", e))),
                            },
                            Err(e) => Err(Error::Storage(format!("invalid multiaddr: {}", e))),
                        };
                        let _ = reply.send(res);
                    }
                    Command::Bootstrap { peers, reply } => {
                        let mut dialled_any = false;
                        let mut last_err: Option<Error> = None;
                        for peer in peers {
                            match peer.parse::<Multiaddr>() {
                                Ok(ma) => {
                                    if let Some(dial_addr) = strip_p2p_suffix(&ma) {
                                        match swarm.dial(dial_addr.clone()) {
                                            Ok(_) => {
                                                dialled_any = true;
                                                if let Some(pid) = ma.iter().find_map(|p| {
                                                    if let Protocol::P2p(hash) = p {
                                                        Some(PeerId::from_multihash(hash.into()).expect("valid multihash"))
                                                    } else { None }
                                                }) {
                                                    swarm.behaviour_mut().kademlia.add_address(&pid, dial_addr);
                                                }
                                            }
                                            Err(e) => {
                                                last_err = Some(Error::Network(format!("dial: {}", e)));
                                            }
                                        }
                                    } else {
                                        last_err = Some(Error::Storage(
                                            "multiaddr missing /p2p/<id> suffix".into(),
                                        ));
                                    }
                                }
                                Err(e) => {
                                    last_err = Some(Error::Storage(format!(
                                        "invalid multiaddr '{}': {}",
                                        peer, e
                                    )));
                                }
                            }
                        }

                        if dialled_any {
                            let _ = swarm.behaviour_mut().kademlia.bootstrap();
                            let _ = reply.send(Ok(()));
                        } else if let Some(err) = last_err {
                            let _ = reply.send(Err(err));
                        } else {
                            let _ = reply.send(Ok(()));
                        }
                    }
                    Command::PutRecord { key, value, reply } => {
                        let record_key = kad::RecordKey::new(&key);
                        let record = kad::Record {
                            key: record_key,
                            value: value.to_vec(),
                            publisher: Some(local_peer_id),
                            expires: Some(std::time::Instant::now() + Duration::from_secs(60 * 60 * 24)),
                        };
                        match swarm.behaviour_mut().kademlia.put_record(record, kad::Quorum::One) {
                            Ok(qid) => {
                                pending_put_record.insert(qid, reply);
                            }
                            Err(e) => {
                                let _ = reply.send(Err(Error::Network(format!(
                                    "kad put_record: {}",
                                    e
                                ))));
                            }
                        }
                    }
                    Command::GetRecord { key, reply } => {
                        let record_key = kad::RecordKey::new(&key);
                        // In libp2p-kad 0.45, get_record returns QueryId directly.
                        let qid = swarm.behaviour_mut().kademlia.get_record(record_key);
                        pending_get_record.insert(qid, reply);
                    }
                    Command::StartProviding { key, reply } => {
                        let record_key = kad::RecordKey::new(&key);
                        match swarm.behaviour_mut().kademlia.start_providing(record_key) {
                            Ok(qid) => {
                                pending_start_providing.insert(qid, reply);
                            }
                            Err(e) => {
                                let _ = reply.send(Err(Error::Network(format!(
                                    "kad start_providing: {}",
                                    e
                                ))));
                            }
                        }
                    }
                    Command::GetProviders { key, reply } => {
                        let record_key = kad::RecordKey::new(&key);
                        // In libp2p-kad 0.45, get_providers returns QueryId directly.
                        let qid = swarm.behaviour_mut().kademlia.get_providers(record_key);
                        pending_get_providers.insert(qid, reply);
                    }
                    Command::LocalPeerId { reply } => {
                        let _ = reply.send(*swarm.local_peer_id());
                    }
                    Command::RegisterServe { key, value } => {
                        serve_map.insert(key, value);
                    }
                    Command::FetchFromPeer { peer, key, reply } => {
                        // Send a request to the peer asking for data by key
                        let request_id = swarm.behaviour_mut().fetch.send_request(&peer, key.into_bytes());
                        pending_fetches.insert(request_id, reply);
                    }
                    Command::Shutdown { reply } => {
                        let _ = reply.send(());
                        return;
                    }
                }
            }

            event = swarm.select_next_some() => {
                handle_swarm_event(
                    event,
                    &mut pending_get_record,
                    &mut pending_get_providers,
                    &mut pending_put_record,
                    &mut pending_start_providing,
                    connected_peers.clone(),
                );
            }
        }
    }
}

fn handle_swarm_event(
    event: SwarmEvent<MyBehaviourEvent>,
    pending_get_record: &mut HashMap<kad::QueryId, oneshot::Sender<Result<Option<Bytes>>>>,
    pending_get_providers: &mut HashMap<kad::QueryId, oneshot::Sender<Result<Vec<PeerId>>>>,
    pending_put_record: &mut HashMap<kad::QueryId, oneshot::Sender<Result<()>>>,
    pending_start_providing: &mut HashMap<kad::QueryId, oneshot::Sender<Result<()>>>,
    connected_peers: Arc<RwLock<Vec<PeerId>>>,
) {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            tracing::info!("P2P listening on {}", address);
        }
        SwarmEvent::ConnectionEstablished {
            peer_id, endpoint, ..
        } => {
            tracing::debug!("P2P connection established: {} via {:?}", peer_id, endpoint);
            let connected = connected_peers.clone();
            tokio::spawn(async move {
                let mut guard = connected.write().await;
                if !guard.contains(&peer_id) {
                    guard.push(peer_id);
                }
            });
        }
        SwarmEvent::ConnectionClosed { peer_id, .. } => {
            tracing::debug!("P2P connection closed: {}", peer_id);
            let connected = connected_peers.clone();
            tokio::spawn(async move {
                connected.write().await.retain(|p| *p != peer_id);
            });
        }
        SwarmEvent::Behaviour(MyBehaviourEvent::Kademlia(
            kad::Event::OutboundQueryProgressed {
                id, result, step, ..
            },
        )) => {
            if let Some(reply) = pending_get_record.remove(&id) {
                match result {
                    kad::QueryResult::GetRecord(Ok(ok)) => match ok {
                        kad::GetRecordOk::FoundRecord(peer_record) => {
                            let _ = reply.send(Ok(Some(Bytes::from(peer_record.record.value))));
                        }
                        kad::GetRecordOk::FinishedWithNoAdditionalRecord { .. } => {
                            if step.last {
                                let _ = reply.send(Ok(None));
                            } else {
                                pending_get_record.insert(id, reply);
                            }
                        }
                    },
                    kad::QueryResult::GetRecord(Err(err)) => {
                        let _ = reply.send(Err(Error::Network(format!("kad get_record: {}", err))));
                    }
                    _ => {
                        pending_get_record.insert(id, reply);
                    }
                }
            } else if let Some(reply) = pending_get_providers.remove(&id) {
                match result {
                    kad::QueryResult::GetProviders(Ok(ok)) => match ok {
                        kad::GetProvidersOk::FoundProviders { providers, .. } => {
                            let v: Vec<PeerId> = providers.into_iter().collect();
                            if step.last {
                                let _ = reply.send(Ok(v));
                            } else {
                                pending_get_providers.insert(id, reply);
                            }
                        }
                        kad::GetProvidersOk::FinishedWithNoAdditionalRecord { .. } => {
                            if step.last {
                                let _ = reply.send(Ok(Vec::new()));
                            } else {
                                pending_get_providers.insert(id, reply);
                            }
                        }
                    },
                    kad::QueryResult::GetProviders(Err(err)) => {
                        let _ =
                            reply.send(Err(Error::Network(format!("kad get_providers: {}", err))));
                    }
                    _ => {
                        pending_get_providers.insert(id, reply);
                    }
                }
            } else if let Some(reply) = pending_put_record.remove(&id) {
                match result {
                    kad::QueryResult::PutRecord(Ok(_)) => {
                        if step.last {
                            let _ = reply.send(Ok(()));
                        } else {
                            pending_put_record.insert(id, reply);
                        }
                    }
                    kad::QueryResult::PutRecord(Err(err)) => {
                        let _ = reply.send(Err(Error::Network(format!("kad put_record: {}", err))));
                    }
                    _ => {
                        pending_put_record.insert(id, reply);
                    }
                }
            } else if let Some(reply) = pending_start_providing.remove(&id) {
                match result {
                    kad::QueryResult::StartProviding(Ok(_)) => {
                        if step.last {
                            let _ = reply.send(Ok(()));
                        } else {
                            pending_start_providing.insert(id, reply);
                        }
                    }
                    kad::QueryResult::StartProviding(Err(err)) => {
                        let _ = reply
                            .send(Err(Error::Network(format!("kad start_providing: {}", err))));
                    }
                    _ => {
                        pending_start_providing.insert(id, reply);
                    }
                }
            }
        }
        SwarmEvent::Behaviour(MyBehaviourEvent::Identify(_)) => {
            // Identify events are useful for NAT-traversal in real deployments;
            // we keep them wired so the protocol is fully functional.
        }
        SwarmEvent::Behaviour(MyBehaviourEvent::Fetch(fetch_event)) => {
            handle_fetch_event(fetch_event, &mut serve_map, &mut pending_fetches);
        }
        SwarmEvent::IncomingConnectionError { error, .. } => {
            tracing::debug!("P2P incoming error: {:?}", error);
        }
        SwarmEvent::OutgoingConnectionError { error, .. } => {
            tracing::debug!("P2P outgoing error: {:?}", error);
        }
        _ => {}
    }
}

/// Handle request-response fetch events.
fn handle_fetch_event(
    event: libp2p::request_response::Event<FetchCodec>,
    serve_map: &mut HashMap<String, Bytes>,
    pending_fetches: &mut HashMap<request_response::RequestId, oneshot::Sender<std::result::Result<Option<Bytes>, Error>>>,
) {
    match event {
        libp2p::request_response::Event::Message { peer, message, .. } => {
            match message {
                // We received a request from a peer asking for data
                request_response::Message::Request {
                    request_id, request, channel, ..
                } => {
                    let key = String::from_utf8_lossy(&request).to_string();
                    if let Some(data) = serve_map.get(&key) {
                        tracing::debug!("Serving fetch request for '{}' to peer {}", key, peer);
                        let _ = channel.send(data.clone().into());
                    } else {
                        tracing::debug!("No data found for fetch request '{}'", key);
                        // Send empty response to indicate no data
                        let _ = channel.send(Vec::new());
                    }
                }
                // We received a response to our fetch request
                request_response::Message::Response {
                    request_id, response, ..
                } => {
                    if let Some(reply) = pending_fetches.remove(&request_id) {
                        if response.is_empty() {
                            let _ = reply.send(Ok(None));
                        } else {
                            let _ = reply.send(Ok(Some(Bytes::from(response))));
                        }
                    }
                }
            }
        }
        libp2p::request_response::Event::OutboundFailure {
            request_id, error, ..
        } => {
            if let Some(reply) = pending_fetches.remove(&request_id) {
                let _ = reply.send(Err(Error::Network(format!("fetch outbound failure: {}", error))));
            }
        }
        libp2p::request_response::Event::InboundFailure { error, .. } => {
            tracing::debug!("Fetch inbound failure: {:?}", error);
        }
    }
}

/// Strip the trailing `/p2p/<id>` from a multiaddr, returning the dialable
/// portion. Returns `None` if the multiaddr is not a peer address.
fn strip_p2p_suffix(addr: &Multiaddr) -> Option<Multiaddr> {
    let mut out = Multiaddr::empty();
    let mut saw_p2p = false;
    for proto in addr.iter() {
        if matches!(proto, Protocol::P2p(_)) {
            saw_p2p = true;
            continue;
        }
        if saw_p2p {
            return None;
        }
        out.push(proto.clone());
    }
    if saw_p2p {
        Some(out)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::BackendConfig;

    #[tokio::test]
    async fn test_p2p_backend_create() {
        let backend = P2PBackend::new().unwrap();
        // peer_id is populated asynchronously; just check the field exists.
        let _ = backend.peer_id().await;
    }

    #[tokio::test]
    async fn test_p2p_put_get() {
        let backend = P2PBackend::new().unwrap();
        backend.init(BackendConfig::default()).await.unwrap();

        let data = Bytes::from(b"hello p2p world".to_vec());
        let _receipt = backend.put("test-key", data.clone()).await.unwrap();

        let retrieved = backend.get("test-key").await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), data);
    }

    #[tokio::test]
    async fn test_p2p_delete() {
        let backend = P2PBackend::new().unwrap();
        backend.init(BackendConfig::default()).await.unwrap();

        backend.put("test-key", Bytes::from("test")).await.unwrap();
        assert!(backend.exists("test-key").await.unwrap());

        backend.delete("test-key").await.unwrap();
        assert!(!backend.exists("test-key").await.unwrap());
    }
}
