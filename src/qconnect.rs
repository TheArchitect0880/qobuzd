use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use prost::encoding::{decode_varint, encode_varint};
use prost::Message;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message as WsMessage},
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::api::{QobuzApi, TrackStream};
use crate::config::Config;
use crate::player::{AudioPlayer, PlayerCommand, PlayerState, StreamSource};
use crate::proto::*;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MSG_TYPE_AUTHENTICATE: u8 = 1;
const MSG_TYPE_SUBSCRIBE: u8 = 2;
const MSG_TYPE_PAYLOAD: u8 = 6;

// ---------------------------------------------------------------------------
// Wire-level prost structs (mirror qbz transport)
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, ::prost::Message)]
struct Authenticate {
    #[prost(uint32, optional, tag = "1")]
    msg_id: Option<u32>,
    #[prost(uint64, optional, tag = "2")]
    msg_date: Option<u64>,
    #[prost(string, optional, tag = "3")]
    jwt: Option<String>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct Subscribe {
    #[prost(uint32, optional, tag = "1")]
    msg_id: Option<u32>,
    #[prost(uint64, optional, tag = "2")]
    msg_date: Option<u64>,
    #[prost(uint32, optional, tag = "3")]
    proto: Option<u32>,
    #[prost(bytes = "vec", repeated, tag = "4")]
    channels: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct CloudPayload {
    #[prost(uint32, optional, tag = "1")]
    msg_id: Option<u32>,
    #[prost(uint64, optional, tag = "2")]
    msg_date: Option<u64>,
    #[prost(uint32, optional, tag = "3")]
    proto: Option<u32>,
    #[prost(bytes = "vec", optional, tag = "4")]
    src: Option<Vec<u8>>,
    #[prost(bytes = "vec", repeated, tag = "5")]
    dests: Vec<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "7")]
    payload: Option<Vec<u8>>,
}

// ---------------------------------------------------------------------------
// Frame / wire helpers
// ---------------------------------------------------------------------------

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn encode_frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(1 + 10 + payload.len());
    frame.push(msg_type);
    encode_varint(payload.len() as u64, &mut frame);
    frame.extend_from_slice(payload);
    frame
}

fn decode_frame(data: &[u8]) -> anyhow::Result<(u8, Vec<u8>)> {
    if data.is_empty() {
        return Err(anyhow::anyhow!("empty qcloud frame"));
    }
    let msg_type = data[0];
    let mut cursor = &data[1..];
    let payload_len = decode_varint(&mut cursor)
        .map_err(|e| anyhow::anyhow!("decode varint: {e}"))? as usize;
    let consumed = data.len().saturating_sub(1 + cursor.len());
    let start = 1 + consumed;
    if data.len() < start + payload_len {
        return Err(anyhow::anyhow!(
            "truncated qcloud frame: expected={}, got={}",
            start + payload_len,
            data.len()
        ));
    }
    Ok((msg_type, data[start..start + payload_len].to_vec()))
}

static BATCH_SEQ: AtomicI32 = AtomicI32::new(1);

fn build_payload_frame(message: QConnectMessage, msg_id: &mut u32) -> Vec<u8> {
    let batch = QConnectMessages {
        messages_time: Some(now_ms()),
        messages_id: Some(BATCH_SEQ.fetch_add(1, Ordering::Relaxed)),
        messages: vec![message],
    };
    let inner = batch.encode_to_vec();

    *msg_id = msg_id.saturating_add(1);
    let cloud = CloudPayload {
        msg_id: Some(*msg_id),
        msg_date: Some(now_ms()),
        proto: Some(1),
        src: None,
        dests: Vec::new(),
        payload: Some(inner),
    };

    encode_frame(MSG_TYPE_PAYLOAD, &cloud.encode_to_vec())
}

fn build_auth_frame(jwt: &str, msg_id: &mut u32) -> Vec<u8> {
    *msg_id = msg_id.saturating_add(1);
    let auth = Authenticate {
        msg_id: Some(*msg_id),
        msg_date: Some(now_ms()),
        jwt: Some(jwt.to_string()),
    };
    encode_frame(MSG_TYPE_AUTHENTICATE, &auth.encode_to_vec())
}

fn build_subscribe_frame(msg_id: &mut u32) -> Vec<u8> {
    *msg_id = msg_id.saturating_add(1);
    let sub = Subscribe {
        msg_id: Some(*msg_id),
        msg_date: Some(now_ms()),
        proto: Some(1),
        channels: vec![vec![0x02]],
    };
    encode_frame(MSG_TYPE_SUBSCRIBE, &sub.encode_to_vec())
}

// ---------------------------------------------------------------------------
// Outbound message builders
// ---------------------------------------------------------------------------

fn device_uuid_bytes(config: &Config) -> Option<Vec<u8>> {
    Uuid::parse_str(&config.device_id)
        .map(|u| u.as_bytes().to_vec())
        .ok()
}

fn make_device_info(config: &Config) -> DeviceInfoMessage {
    DeviceInfoMessage {
        device_uuid: device_uuid_bytes(config),
        friendly_name: Some(config.device_name.clone()),
        brand: Some("QobuzD".to_string()),
        model: Some("Linux".to_string()),
        serial_number: None,
        device_type: Some(5),
        capabilities: None,
        software_version: Some("qobuzd-0.1.0".to_string()),
    }
}

fn build_ctrl_join_session(config: &Config) -> QConnectMessage {
    QConnectMessage {
        message_type: Some(QConnectMessageType::MessageTypeCtrlSrvrJoinSession as i32),
        ctrl_srvr_join_session: Some(JoinSessionMessage {
            session_uuid: None,
            device_info: Some(make_device_info(config)),
            reason: None,
            initial_state: None,
            is_active: None,
        }),
        ..Default::default()
    }
}

fn build_renderer_join_session(
    session_uuid: Vec<u8>,
    config: &Config,
    playing_state: i32,
) -> QConnectMessage {
    QConnectMessage {
        message_type: Some(QConnectMessageType::MessageTypeRndrSrvrJoinSession as i32),
        rndr_srvr_join_session: Some(JoinSessionMessage {
            session_uuid: Some(session_uuid),
            device_info: Some(make_device_info(config)),
            reason: None,
            initial_state: Some(RendererStateMessage {
                playing_state: Some(playing_state),
                buffer_state: Some(if playing_state == 2 || playing_state == 3 {
                    2
                } else {
                    1
                }),
                current_position: None,
                duration: None,
                queue_version: Some(QueueVersionRef {
                    major: Some(0),
                    minor: Some(0),
                }),
                current_queue_item_id: None,
                next_queue_item_id: None,
            }),
            is_active: Some(true),
        }),
        ..Default::default()
    }
}

fn build_state_updated(
    playing_state: i32,
    position_ms: u64,
    duration_ms: u64,
    queue_item_id: i32,
    next_queue_item_id: i32,
) -> QConnectMessage {
    let current_qid = if queue_item_id <= 0 {
        None
    } else {
        Some(queue_item_id)
    };
    let next_qid = if next_queue_item_id <= 0 {
        None
    } else {
        Some(next_queue_item_id)
    };

    QConnectMessage {
        message_type: Some(QConnectMessageType::MessageTypeRndrSrvrStateUpdated as i32),
        rndr_srvr_state_updated: Some(RendererStateUpdatedMessage {
            state: Some(RendererStateMessage {
                playing_state: Some(playing_state),
                buffer_state: Some(if playing_state == 2 || playing_state == 3 {
                    2
                } else {
                    1
                }),
                current_position: Some(PlaybackPositionMessage {
                    timestamp: Some(now_ms()),
                    value: Some(position_ms as i32),
                }),
                duration: if duration_ms > 0 {
                    Some(duration_ms as i32)
                } else {
                    None
                },
                queue_version: Some(QueueVersionRef {
                    major: Some(0),
                    minor: Some(0),
                }),
                current_queue_item_id: current_qid,
                next_queue_item_id: next_qid,
            }),
        }),
        ..Default::default()
    }
}

fn build_volume_changed(volume: i32) -> QConnectMessage {
    QConnectMessage {
        message_type: Some(QConnectMessageType::MessageTypeRndrSrvrVolumeChanged as i32),
        rndr_srvr_volume_changed: Some(RendererVolumeChangedMessage {
            volume: Some(volume),
        }),
        ..Default::default()
    }
}

fn build_volume_muted(value: bool) -> QConnectMessage {
    QConnectMessage {
        message_type: Some(QConnectMessageType::MessageTypeRndrSrvrVolumeMuted as i32),
        rndr_srvr_volume_muted: Some(RendererVolumeMutedMessage { value: Some(value) }),
        ..Default::default()
    }
}

fn build_file_quality_changed(sr: i32, bits: i32, ch: i32, quality: i32) -> QConnectMessage {
    QConnectMessage {
        message_type: Some(
            QConnectMessageType::MessageTypeRndrSrvrFileAudioQualityChanged as i32,
        ),
        rndr_srvr_file_audio_quality_changed: Some(RendererFileAudioQualityChangedMessage {
            sampling_rate: Some(sr),
            bit_depth: Some(bits),
            nb_channels: Some(ch),
            audio_quality: Some(quality),
        }),
        ..Default::default()
    }
}

fn build_renderer_action_next() -> QConnectMessage {
    QConnectMessage {
        message_type: Some(QConnectMessageType::MessageTypeRndrSrvrRendererAction as i32),
        rndr_srvr_renderer_action: Some(RendererActionMessage {
            seek_position: None,
            action: 2, // ACTION_TYPE_NEXT
        }),
        ..Default::default()
    }
}

fn build_max_quality_changed(max_quality: i32) -> QConnectMessage {
    QConnectMessage {
        message_type: Some(
            QConnectMessageType::MessageTypeRndrSrvrMaxAudioQualityChanged as i32,
        ),
        rndr_srvr_max_audio_quality_changed: Some(RendererMaxAudioQualityChangedMessage {
            max_audio_quality: Some(max_quality),
            network_type: None,
        }),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Quality helpers
// ---------------------------------------------------------------------------

fn quality_to_format_id(quality: i32) -> u32 {
    match quality {
        1 => 5,      // MP3 320kbps
        2 => 6,      // FLAC CD 16-bit/44.1kHz
        3 => 7,      // FLAC Hi-Res 24-bit/96kHz
        4 | 5 => 27, // FLAC Hi-Res 24-bit/192kHz
        _ => 27,     // default to max quality
    }
}

fn quality_fallback_audio_params(quality: i32) -> (u32, u32, u32) {
    match quality {
        1 => (44100, 16, 2),      // MP3
        2 => (44100, 16, 2),      // CD
        3 => (96000, 24, 2),      // Hi-Res up to 96kHz
        4 | 5 => (192000, 24, 2), // Hi-Res up to 192/384kHz
        _ => (44100, 16, 2),
    }
}

// ---------------------------------------------------------------------------
// Local renderer state
// ---------------------------------------------------------------------------

struct RendererLocal {
    current_track_id: i32,
    current_queue_item_id: i32,
    current_next_queue_item_id: i32,
    current_duration_ms: u64,
    current_playing_state: i32, // 1=stopped, 2=playing, 3=paused
    max_audio_quality: i32,     // default: 4
    volume: u8,                 // default: 100
    pre_mute_volume: u8,        // default: 100
    muted: bool,
    track_ended: bool,
    has_position_progress: bool,
    last_play_at: Instant,
    ignore_seek_until: Option<Instant>,
    joined: bool,
}

impl Default for RendererLocal {
    fn default() -> Self {
        Self {
            current_track_id: 0,
            current_queue_item_id: -1,
            current_next_queue_item_id: -1,
            current_duration_ms: 0,
            current_playing_state: 1,
            max_audio_quality: 4,
            volume: 100,
            pre_mute_volume: 100,
            muted: false,
            track_ended: false,
            has_position_progress: false,
            last_play_at: Instant::now(),
            ignore_seek_until: None,
            joined: false,
        }
    }
}

// ---------------------------------------------------------------------------
// RendererHandler
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct RendererHandler {
    api: Arc<QobuzApi>,
    auth_token: Arc<String>,
    player: Arc<AudioPlayer>,
    local: Arc<Mutex<RendererLocal>>,
    out_tx: mpsc::Sender<QConnectMessage>,
    config: Arc<Config>,
}

impl RendererHandler {
    async fn send(&self, msg: QConnectMessage) {
        if let Err(e) = self.out_tx.send(msg).await {
            warn!("[QConnect] Failed to queue outbound message: {e}");
        }
    }

    async fn join_session(&self, session_uuid: Vec<u8>) {
        let (playing_state, already_joined) = {
            let local = self.local.lock().await;
            (local.current_playing_state, local.joined)
        };
        if already_joined {
            return;
        }

        info!("[QConnect] Sending renderer join");
        let msg = build_renderer_join_session(session_uuid, &self.config, playing_state);
        self.send(msg).await;

        {
            let mut local = self.local.lock().await;
            local.joined = true;
        }

        // Send initial volume report
        let vol = self.local.lock().await.volume;
        self.send(build_volume_changed(vol as i32)).await;
        info!("[QConnect] Renderer joined, initial volume={vol} sent");
    }

    async fn send_state_report(
        &self,
        playing_state: i32,
        position_ms: u64,
        duration_ms: u64,
        queue_item_id: i32,
        next_queue_item_id: i32,
    ) {
        debug!(
            "[QConnect] StateReport: playing={playing_state} pos={position_ms}ms dur={duration_ms}ms qi={queue_item_id} nqi={next_queue_item_id}"
        );
        self.send(build_state_updated(
            playing_state,
            position_ms,
            duration_ms,
            queue_item_id,
            next_queue_item_id,
        ))
        .await;
    }

    async fn dispatch_inbound(&self, batch: QConnectMessages) {
        for msg in batch.messages {
            let Some(mt) = msg.message_type.or_else(|| resolve_message_type_from_fields(&msg))
            else {
                continue;
            };

            if mt == QConnectMessageType::MessageTypeSrvrCtrlSessionState as i32 {
                if let Some(ss) = msg.srvr_ctrl_session_state {
                    if let Some(uuid_bytes) = ss.session_uuid {
                        let should_join = !self.local.lock().await.joined;
                        if should_join {
                            info!("[QConnect] Got session UUID from SessionState");
                            let h = self.clone();
                            tokio::spawn(async move {
                                h.join_session(uuid_bytes).await;
                            });
                        }
                    }
                }
                continue;
            }

            if mt == QConnectMessageType::MessageTypeSrvrRndrSetState as i32 {
                if let Some(payload) = msg.srvr_rndr_set_state {
                    // Echo detection: server echoes our state reports as SET_STATE with only
                    // next_track set. Skip those — but not seek commands (current_position set).
                    if payload.playing_state.is_none()
                        && payload.current_track.is_none()
                        && payload.current_position.is_none()
                    {
                        debug!("[QConnect] Skipping echo SET_STATE");
                        continue;
                    }
                    let h = self.clone();
                    tokio::spawn(async move {
                        h.handle_set_state(payload).await;
                    });
                }
                continue;
            }

            if mt == QConnectMessageType::MessageTypeSrvrRndrSetVolume as i32 {
                if let Some(payload) = msg.srvr_rndr_set_volume {
                    let h = self.clone();
                    tokio::spawn(async move {
                        h.handle_set_volume(payload).await;
                    });
                }
                continue;
            }

            if mt == QConnectMessageType::MessageTypeSrvrRndrSetActive as i32 {
                if let Some(payload) = msg.srvr_rndr_set_active {
                    let h = self.clone();
                    tokio::spawn(async move {
                        h.handle_set_active(payload).await;
                    });
                }
                continue;
            }

            if mt == QConnectMessageType::MessageTypeSrvrRndrSetMaxAudioQuality as i32 {
                if let Some(payload) = msg.srvr_rndr_set_max_audio_quality {
                    let h = self.clone();
                    tokio::spawn(async move {
                        h.handle_set_max_audio_quality(payload).await;
                    });
                }
                continue;
            }

            if mt == QConnectMessageType::MessageTypeSrvrRndrSetLoopMode as i32 {
                if let Some(payload) = msg.srvr_rndr_set_loop_mode {
                    info!("[QConnect] SetLoopMode: {:?}", payload.loop_mode);
                }
                continue;
            }

            if mt == QConnectMessageType::MessageTypeSrvrRndrSetShuffleMode as i32 {
                if let Some(payload) = msg.srvr_rndr_set_shuffle_mode {
                    info!("[QConnect] SetShuffleMode: {:?}", payload.shuffle_mode);
                }
                continue;
            }

            if mt == QConnectMessageType::MessageTypeSrvrRndrMuteVolume as i32 {
                if let Some(payload) = msg.srvr_rndr_mute_volume {
                    let h = self.clone();
                    tokio::spawn(async move {
                        h.handle_mute_volume(payload).await;
                    });
                }
                continue;
            }
        }
    }

    async fn handle_set_state(&self, payload: RendererSetStateMessage) {
        let playing_state = payload.playing_state;
        let current_position_ms = payload.current_position.map(|p| p.max(0) as u64);
        let current_track = payload.current_track;
        let next_track = payload.next_track;

        info!(
            "[QConnect] SET_STATE: playing_state={:?} pos={:?} current_track={:?} next_track={:?}",
            playing_state,
            current_position_ms,
            current_track
                .as_ref()
                .map(|t| (t.track_id, t.queue_item_id)),
            next_track.as_ref().map(|t| (t.track_id, t.queue_item_id)),
        );

        // Store next_track metadata
        if let Some(nt) = &next_track {
            let mut local = self.local.lock().await;
            local.current_next_queue_item_id = nt.queue_item_id.unwrap_or(-1);
        }

        // Seek-only detection (no playing_state, no current/next track, position present)
        let seek_only = playing_state.is_none()
            && current_track.is_none()
            && next_track.is_none()
            && current_position_ms.is_some();

        if seek_only {
            let target_pos = current_position_ms.unwrap_or(0);
            let player_pos = self.player.status().position_ms;

            let (qi, nqi, ps, dur) = {
                let mut local = self.local.lock().await;
                let suppress = target_pos > 0
                    && local
                        .ignore_seek_until
                        .map(|deadline| Instant::now() < deadline)
                        .unwrap_or(false);
                let should_seek =
                    !suppress && (target_pos == 0 || target_pos.abs_diff(player_pos) > 350);
                if should_seek {
                    self.player.send(PlayerCommand::Seek(target_pos));
                    local.track_ended = false;
                    if target_pos == 0 {
                        local.ignore_seek_until =
                            Some(Instant::now() + Duration::from_secs(2));
                    }
                }
                info!(
                    "[QConnect] seek-only: target={target_pos}ms local={player_pos}ms should_seek={should_seek} suppressed={suppress}"
                );
                (
                    local.current_queue_item_id,
                    local.current_next_queue_item_id,
                    local.current_playing_state,
                    local.current_duration_ms,
                )
            };
            self.send_state_report(ps, target_pos, dur, qi, nqi).await;
            return;
        }

        // New track detection
        // Sentinel value: track_id=u32::MAX means "no track" (queue cleared by another client)
        let queue_cleared = current_track
            .as_ref()
            .map_or(false, |t| t.track_id.unwrap_or(0) == u32::MAX);

        if queue_cleared {
            debug!("[QConnect] Queue cleared (sentinel track), stopping playback");
            self.player.send(PlayerCommand::Stop);
            let mut local = self.local.lock().await;
            local.current_track_id = -1;
            local.current_queue_item_id = -1;
            local.current_next_queue_item_id = -1;
            local.current_playing_state = 1;
            local.current_duration_ms = 0;
            local.track_ended = false;
            return;
        }

        let new_track_info: Option<(i32, i32, i32, u64, i32)> = {
            let local = self.local.lock().await;
            if let Some(track) = &current_track {
                let tid = track.track_id.unwrap_or(0) as i32;
                let qid = track.queue_item_id.unwrap_or(-1);
                if tid != local.current_track_id || qid != local.current_queue_item_id {
                    Some((
                        tid,
                        qid,
                        local.max_audio_quality,
                        current_position_ms.unwrap_or(0),
                        local.current_next_queue_item_id,
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some((tid, qid, max_q, start_pos, nqi_snap)) = new_track_info {
            info!("[QConnect] Loading new track {tid} (qi={qid})");
            {
                let mut local = self.local.lock().await;
                local.current_track_id = tid;
                local.current_queue_item_id = qid;
                local.current_playing_state = 2;
                local.current_duration_ms = 0;
                local.last_play_at = Instant::now();
                local.has_position_progress = false;
                local.track_ended = false;
                local.ignore_seek_until = None;
            }

            // Send BUFFERING state immediately
            self.send_state_report(2, start_pos, 0, qid, nqi_snap).await;

            let track_id_str = tid.to_string();
            let format_id = quality_to_format_id(max_q);

            let duration_ms = match self.api.get_track(&self.auth_token, &track_id_str).await {
                Ok(t) => t.duration.unwrap_or(0) as u64 * 1000,
                Err(e) => {
                    warn!("[QConnect] get_track failed: {e}");
                    0
                }
            };

            match self
                .api
                .get_track_stream(&self.auth_token, &track_id_str, format_id)
                .await
            {
                Ok(stream) => {
                    let (player_stream, stream_sr, stream_bits, stream_ch) = match stream {
                        TrackStream::DirectUrl {
                            url,
                            sampling_rate_hz,
                            bit_depth,
                            channels,
                        } => {
                            info!(
                                "[QConnect] Got direct stream URL (duration={duration_ms}ms)"
                            );
                            (
                                StreamSource::DirectUrl(url),
                                sampling_rate_hz,
                                bit_depth,
                                channels,
                            )
                        }
                        TrackStream::Segmented {
                            url_template,
                            n_segments,
                            encryption_key_hex,
                            sampling_rate_hz,
                            bit_depth,
                            channels,
                        } => {
                            info!("[QConnect] Got segmented stream (segments={n_segments}, duration={duration_ms}ms)");
                            (
                                StreamSource::Segmented {
                                    url_template,
                                    n_segments,
                                    encryption_key_hex,
                                },
                                sampling_rate_hz,
                                bit_depth,
                                channels,
                            )
                        }
                    };

                    self.player.send(PlayerCommand::Play {
                        stream: player_stream,
                        track_id: tid,
                        queue_item_id: qid,
                        duration_ms,
                        start_position_ms: start_pos,
                    });

                    {
                        let mut loc = self.local.lock().await;
                        loc.current_duration_ms = duration_ms;
                    }

                    let (fallback_sr, fallback_bits, fallback_ch) =
                        quality_fallback_audio_params(max_q);
                    let sr = stream_sr.unwrap_or(fallback_sr).max(1);
                    let bits = stream_bits.unwrap_or(fallback_bits).max(1);
                    let ch = stream_ch.unwrap_or(fallback_ch).max(1);

                    self.send(build_file_quality_changed(
                        sr as i32,
                        bits as i32,
                        ch as i32,
                        max_q,
                    ))
                    .await;
                    self.send(build_max_quality_changed(max_q)).await;
                    self.send_state_report(2, start_pos, duration_ms, qid, nqi_snap)
                        .await;
                }
                Err(e) => {
                    error!("[QConnect] Failed to get stream URL: {e}");
                    {
                        let mut loc = self.local.lock().await;
                        loc.current_playing_state = 1;
                    }
                    self.send_state_report(1, 0, 0, qid, nqi_snap).await;
                }
            }
            return;
        }

        // playing_state only (no new track)
        if let Some(ps) = playing_state {
            let start_pos = current_position_ms.unwrap_or(0);
            let (cur_ps, cur_pos, dur, qi, nqi) = {
                let mut local = self.local.lock().await;
                match ps {
                    2 => {
                        let status = self.player.status();
                        let should_restart = current_track.is_some()
                            && (local.track_ended || status.state == PlayerState::Stopped);

                        if should_restart {
                            info!(
                                "[QConnect] Restarting current track from {start_pos}ms (ended={} state={:?})",
                                local.track_ended, status.state
                            );
                            self.player.send(PlayerCommand::Seek(start_pos));
                            local.current_playing_state = 2;
                            local.track_ended = false;
                        } else if local.current_playing_state == 3 {
                            info!("[QConnect] Resuming playback");
                            self.player.send(PlayerCommand::Resume);
                            local.current_playing_state = 2;
                            local.track_ended = false;
                        } else if local.current_playing_state != 2 {
                            info!(
                                "[QConnect] Play requested, state={}",
                                local.current_playing_state
                            );
                            local.current_playing_state = 2;
                        }
                    }
                    3 => {
                        info!("[QConnect] Pausing playback");
                        self.player.send(PlayerCommand::Pause);
                        local.current_playing_state = 3;
                    }
                    1 => {
                        info!("[QConnect] Stopping playback");
                        self.player.send(PlayerCommand::Stop);
                        local.current_playing_state = 1;
                        local.current_queue_item_id = -1;
                        local.current_next_queue_item_id = -1;
                        local.current_track_id = 0;
                        local.track_ended = false;
                    }
                    _ => {}
                }

                let pos_out = match ps {
                    1 => 0,
                    _ => current_position_ms.unwrap_or_else(|| self.player.status().position_ms),
                };
                (
                    local.current_playing_state,
                    pos_out,
                    local.current_duration_ms,
                    local.current_queue_item_id,
                    local.current_next_queue_item_id,
                )
            };

            self.send_state_report(cur_ps, cur_pos, dur, qi, nqi).await;
            return;
        }

        // Position control with no playing_state and no track change
        let (ps, pos, dur, qi, nqi) = {
            let mut local = self.local.lock().await;
            if let Some(p) = current_position_ms {
                let player_pos = self.player.status().position_ms;
                let suppress = p > 0
                    && local
                        .ignore_seek_until
                        .map(|deadline| Instant::now() < deadline)
                        .unwrap_or(false);
                let should_seek = !suppress && (p == 0 || p.abs_diff(player_pos) > 350);
                if should_seek {
                    info!("[QConnect] Position jump: seeking to {p}ms (local={player_pos}ms)");
                    self.player.send(PlayerCommand::Seek(p));
                    local.track_ended = false;
                    if p == 0 {
                        local.ignore_seek_until =
                            Some(Instant::now() + Duration::from_secs(2));
                    }
                }
            }
            let pos_out =
                current_position_ms.unwrap_or_else(|| self.player.status().position_ms);
            (
                local.current_playing_state,
                pos_out,
                local.current_duration_ms,
                local.current_queue_item_id,
                local.current_next_queue_item_id,
            )
        };

        self.send_state_report(ps, pos, dur, qi, nqi).await;
    }

    async fn handle_set_volume(&self, payload: RendererSetVolumeMessage) {
        let mut local = self.local.lock().await;
        let new_vol = if let Some(v) = payload.volume {
            v.clamp(0, 100) as u8
        } else if let Some(d) = payload.volume_delta {
            (local.volume as i32 + d).clamp(0, 100) as u8
        } else {
            local.volume
        };
        info!("[QConnect] SetVolume: {new_vol}");
        local.volume = new_vol;
        if local.muted && new_vol > 0 {
            local.muted = false;
        }
        drop(local);
        self.player.send(PlayerCommand::SetVolume(new_vol));
        self.send(build_volume_changed(new_vol as i32)).await;
    }

    async fn handle_set_active(&self, payload: RendererSetActiveMessage) {
        let active = payload.active.unwrap_or(true);
        info!("[QConnect] SetActive: {active}");
        if !active {
            let mut local = self.local.lock().await;
            local.current_playing_state = 1;
            local.current_queue_item_id = -1;
            local.current_next_queue_item_id = -1;
            local.current_track_id = 0;
            let nqi = local.current_next_queue_item_id;
            drop(local);
            self.player.send(PlayerCommand::Stop);
            self.send_state_report(1, 0, 0, -1, nqi).await;
        }
    }

    async fn handle_mute_volume(&self, payload: RendererMuteVolumeMessage) {
        let value = payload.value.unwrap_or(false);
        info!("[QConnect] MuteVolume: {value}");
        let mut local = self.local.lock().await;
        if value {
            local.pre_mute_volume = local.volume;
            local.volume = 0;
            local.muted = true;
        } else {
            local.volume = local.pre_mute_volume;
            local.muted = false;
        }
        let vol = local.volume;
        drop(local);
        self.player.send(PlayerCommand::SetVolume(vol));
        self.send(build_volume_muted(value)).await;
    }

    async fn handle_set_max_audio_quality(&self, payload: RendererSetMaxAudioQualityMessage) {
        let max_audio_quality = payload.max_audio_quality.unwrap_or(4);
        let format_id = quality_to_format_id(max_audio_quality);
        info!("[QConnect] SetMaxAudioQuality: {max_audio_quality} (format_id={format_id})");

        {
            let mut local = self.local.lock().await;
            local.max_audio_quality = max_audio_quality;
        }

        self.send(build_max_quality_changed(max_audio_quality)).await;

        let (tid, qi, nqi, cur_ps, dur) = {
            let local = self.local.lock().await;
            (
                local.current_track_id,
                local.current_queue_item_id,
                local.current_next_queue_item_id,
                local.current_playing_state,
                local.current_duration_ms,
            )
        };

        if cur_ps == 2 && tid != 0 {
            let restart_pos = self.player.status().position_ms;
            info!("[QConnect] Restarting track {tid} at new quality format_id={format_id}");

            self.send_state_report(2, restart_pos, dur, qi, nqi).await;

            let track_id_str = tid.to_string();
            let duration_ms = match self.api.get_track(&self.auth_token, &track_id_str).await {
                Ok(t) => t.duration.unwrap_or(0) as u64 * 1000,
                Err(e) => {
                    warn!("[QConnect] get_track failed: {e}");
                    dur
                }
            };

            match self
                .api
                .get_track_stream(&self.auth_token, &track_id_str, format_id)
                .await
            {
                Ok(stream) => {
                    let (player_stream, stream_sr, stream_bits, stream_ch) = match stream {
                        TrackStream::DirectUrl {
                            url,
                            sampling_rate_hz,
                            bit_depth,
                            channels,
                        } => (StreamSource::DirectUrl(url), sampling_rate_hz, bit_depth, channels),
                        TrackStream::Segmented {
                            url_template,
                            n_segments,
                            encryption_key_hex,
                            sampling_rate_hz,
                            bit_depth,
                            channels,
                        } => (
                            StreamSource::Segmented {
                                url_template,
                                n_segments,
                                encryption_key_hex,
                            },
                            sampling_rate_hz,
                            bit_depth,
                            channels,
                        ),
                    };

                    self.player.send(PlayerCommand::Play {
                        stream: player_stream,
                        track_id: tid,
                        queue_item_id: qi,
                        duration_ms,
                        start_position_ms: restart_pos,
                    });

                    {
                        let mut loc = self.local.lock().await;
                        loc.current_duration_ms = duration_ms;
                    }

                    let (fallback_sr, fallback_bits, fallback_ch) =
                        quality_fallback_audio_params(max_audio_quality);
                    let sr = stream_sr.unwrap_or(fallback_sr).max(1);
                    let bits = stream_bits.unwrap_or(fallback_bits).max(1);
                    let ch = stream_ch.unwrap_or(fallback_ch).max(1);

                    self.send(build_file_quality_changed(
                        sr as i32,
                        bits as i32,
                        ch as i32,
                        max_audio_quality,
                    ))
                    .await;
                    self.send(build_max_quality_changed(max_audio_quality)).await;
                    self.send_state_report(2, restart_pos, duration_ms, qi, nqi)
                        .await;
                    info!("[QConnect] Restarted at format_id={format_id}");
                }
                Err(e) => {
                    error!("[QConnect] Failed to get stream for quality change: {e}");
                    let mut loc = self.local.lock().await;
                    loc.current_playing_state = 1;
                    drop(loc);
                    self.send_state_report(1, 0, duration_ms, qi, nqi).await;
                }
            }
        }
    }
}

// Fallback: resolve message type from which field is populated (mirrors qbz decoder)
fn resolve_message_type_from_fields(msg: &QConnectMessage) -> Option<i32> {
    if msg.srvr_rndr_set_state.is_some() {
        return Some(QConnectMessageType::MessageTypeSrvrRndrSetState as i32);
    }
    if msg.srvr_rndr_set_volume.is_some() {
        return Some(QConnectMessageType::MessageTypeSrvrRndrSetVolume as i32);
    }
    if msg.srvr_rndr_set_active.is_some() {
        return Some(QConnectMessageType::MessageTypeSrvrRndrSetActive as i32);
    }
    if msg.srvr_rndr_set_max_audio_quality.is_some() {
        return Some(QConnectMessageType::MessageTypeSrvrRndrSetMaxAudioQuality as i32);
    }
    if msg.srvr_rndr_set_loop_mode.is_some() {
        return Some(QConnectMessageType::MessageTypeSrvrRndrSetLoopMode as i32);
    }
    if msg.srvr_rndr_set_shuffle_mode.is_some() {
        return Some(QConnectMessageType::MessageTypeSrvrRndrSetShuffleMode as i32);
    }
    if msg.srvr_rndr_mute_volume.is_some() {
        return Some(QConnectMessageType::MessageTypeSrvrRndrMuteVolume as i32);
    }
    if msg.srvr_ctrl_session_state.is_some() {
        return Some(QConnectMessageType::MessageTypeSrvrCtrlSessionState as i32);
    }
    None
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

pub struct QConnect {
    _handle: tokio::task::JoinHandle<()>,
}

impl QConnect {
    pub fn start(auth_token: String, _device_uuid: String, _device_name: String) -> Self {
        let handle = tokio::spawn(async move {
            let config = match Config::load() {
                Ok(c) => c,
                Err(e) => {
                    error!("[QConnect] Failed to load config: {e}");
                    return;
                }
            };
            run_qconnect(auth_token, config).await;
        });
        Self { _handle: handle }
    }
}

pub async fn run_qconnect(auth_token: String, config: Config) {
    let mut backoff = 5u64;
    loop {
        info!("[QConnect] Connecting...");
        match run_qconnect_once(&auth_token, &config).await {
            Ok(()) => {
                info!("[QConnect] Disconnected cleanly");
                backoff = 5;
            }
            Err(e) => {
                error!("[QConnect] Error: {e}");
            }
        }
        info!("[QConnect] Reconnecting in {backoff}s");
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(120);
    }
}

async fn run_qconnect_once(auth_token: &str, config: &Config) -> anyhow::Result<()> {
    let api = Arc::new(QobuzApi::new(config));
    let token_resp = api.get_qws_token(auth_token).await?;
    let jwt = token_resp.jwt_qws.jwt;
    let endpoint = token_resp.jwt_qws.endpoint;

    info!("[QConnect] Connecting to {endpoint}");

    let (ws, _) = connect_async_with_config(
        &endpoint,
        Some(WebSocketConfig::default()),
        false,
    )
    .await
    .map_err(|e| anyhow::anyhow!("WebSocket connect failed: {e}"))?;

    let (mut ws_write, mut ws_read) = ws.split();
    let mut msg_id: u32 = 0;

    // Authenticate
    let auth_frame = build_auth_frame(&jwt, &mut msg_id);
    ws_write
        .send(WsMessage::Binary(auth_frame.into()))
        .await
        .map_err(|e| anyhow::anyhow!("auth send failed: {e}"))?;

    // Subscribe
    let sub_frame = build_subscribe_frame(&mut msg_id);
    ws_write
        .send(WsMessage::Binary(sub_frame.into()))
        .await
        .map_err(|e| anyhow::anyhow!("subscribe send failed: {e}"))?;

    let player = Arc::new(AudioPlayer::new());
    let local = Arc::new(Mutex::new(RendererLocal::default()));
    let (out_tx, mut out_rx) = mpsc::channel::<QConnectMessage>(64);

    let handler = RendererHandler {
        api: Arc::clone(&api),
        auth_token: Arc::new(auth_token.to_string()),
        player: Arc::clone(&player),
        local: Arc::clone(&local),
        out_tx,
        config: Arc::new(config.clone()),
    };

    // Send ctrl join after subscribe
    {
        let ctrl_join = build_ctrl_join_session(config);
        let frame = build_payload_frame(ctrl_join, &mut msg_id);
        ws_write
            .send(WsMessage::Binary(frame.into()))
            .await
            .map_err(|e| anyhow::anyhow!("ctrl join send failed: {e}"))?;
        info!("[QConnect] Subscribed — sent CtrlSrvrJoinSession");
    }

    let mut position_ticker = tokio::time::interval(Duration::from_millis(500));
    position_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = position_ticker.tick() => {
                let (playing_state, last_play_at, has_progress, track_ended_local, dur, qi, nqi) = {
                    let loc = local.lock().await;
                    (
                        loc.current_playing_state,
                        loc.last_play_at,
                        loc.has_position_progress,
                        loc.track_ended,
                        loc.current_duration_ms,
                        loc.current_queue_item_id,
                        loc.current_next_queue_item_id,
                    )
                };

                if playing_state == 2 {
                    let status = player.status();
                    let elapsed = last_play_at.elapsed();

                    if status.state == PlayerState::Stopped
                        && has_progress
                        && elapsed > Duration::from_secs(3)
                    {
                        if !track_ended_local {
                            info!("[QConnect] Track ended naturally, nqi={nqi}");
                            {
                                let mut loc = local.lock().await;
                                loc.track_ended = true;
                                loc.current_playing_state = 1;
                            }
                            handler.send_state_report(1, dur, dur, qi, nqi).await;
                            if nqi > 0 {
                                // There is a next track — ask server to advance
                                handler.send(build_renderer_action_next()).await;
                            }
                            // If nqi <= 0, just stay stopped; server will keep loop mode off
                        }
                    } else if status.state != PlayerState::Stopped {
                        let new_pos = status.position_ms;
                        if new_pos > 0 {
                            {
                                let mut loc = local.lock().await;
                                if !loc.has_position_progress {
                                    loc.has_position_progress = true;
                                    info!("[QConnect] First position progress: {new_pos}ms");
                                }
                            }
                            // Periodic position report so the app's progress bar updates
                            handler.send_state_report(2, new_pos, dur, qi, nqi).await;
                        }
                    }
                }
            }

            incoming = ws_read.next() => {
                match incoming {
                    Some(Ok(WsMessage::Binary(data))) => {
                        match decode_frame(&data) {
                            Ok((MSG_TYPE_PAYLOAD, payload)) => {
                                match CloudPayload::decode(payload.as_slice()) {
                                    Ok(cloud) => {
                                        let inner = cloud.payload.unwrap_or_default();
                                        match QConnectMessages::decode(inner.as_slice()) {
                                            Ok(batch) => {
                                                handler.dispatch_inbound(batch).await;
                                            }
                                            Err(e) => {
                                                debug!("[QConnect] QConnectMessages decode error: {e}");
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        debug!("[QConnect] CloudPayload decode error: {e}");
                                    }
                                }
                            }
                            Ok((MSG_TYPE_AUTHENTICATE, _)) => {
                                debug!("[QConnect] Auth ack received");
                            }
                            Ok((MSG_TYPE_SUBSCRIBE, _)) => {
                                debug!("[QConnect] Subscribe ack received");
                            }
                            Ok((9, _)) => {
                                return Err(anyhow::anyhow!("server sent error frame"));
                            }
                            Ok((10, _)) => {
                                return Err(anyhow::anyhow!("server sent disconnect frame"));
                            }
                            Ok(_) => {}
                            Err(e) => {
                                warn!("[QConnect] Frame decode error: {e}");
                            }
                        }
                    }
                    Some(Ok(WsMessage::Ping(payload))) => {
                        let _ = ws_write.send(WsMessage::Pong(payload)).await;
                    }
                    Some(Ok(WsMessage::Pong(_))) => {}
                    Some(Ok(WsMessage::Close(_))) => {
                        return Err(anyhow::anyhow!("server closed connection"));
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        return Err(anyhow::anyhow!("WebSocket read error: {e}"));
                    }
                    None => {
                        return Err(anyhow::anyhow!("WebSocket stream ended"));
                    }
                }
            }

            Some(msg) = out_rx.recv() => {
                let frame = build_payload_frame(msg, &mut msg_id);
                if let Err(e) = ws_write.send(WsMessage::Binary(frame.into())).await {
                    return Err(anyhow::anyhow!("WebSocket write error: {e}"));
                }
            }
        }
    }
}
