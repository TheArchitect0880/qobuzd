use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::api::{QobuzApi, TrackStream};
use crate::config::Config;
use crate::player::{AudioPlayer, PlayerCommand, PlayerState, StreamSource};

// ---------------------------------------------------------------------------
// Protobuf helpers (hand-rolled, matching the qconnect.proto schema)
// ---------------------------------------------------------------------------

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn encode_varint(mut val: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10);
    loop {
        let mut byte = (val & 0x7F) as u8;
        val >>= 7;
        if val != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if val == 0 {
            break;
        }
    }
    buf
}

fn encode_field_varint(field: u32, val: u64) -> Vec<u8> {
    let tag = (field as u64) << 3;
    let mut out = encode_varint(tag);
    out.extend(encode_varint(val));
    out
}

fn encode_field_bytes(field: u32, data: &[u8]) -> Vec<u8> {
    let tag = ((field as u64) << 3) | 2;
    let mut out = encode_varint(tag);
    out.extend(encode_varint(data.len() as u64));
    out.extend_from_slice(data);
    out
}

fn encode_field_string(field: u32, s: &str) -> Vec<u8> {
    encode_field_bytes(field, s.as_bytes())
}

fn encode_field_fixed64(field: u32, val: u64) -> Vec<u8> {
    let tag = ((field as u64) << 3) | 1;
    let mut out = encode_varint(tag);
    out.extend_from_slice(&val.to_le_bytes());
    out
}

fn decode_varint(data: &[u8]) -> Option<(u64, usize)> {
    let mut val: u64 = 0;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        val |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((val, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

/// Parsed protobuf field: (field_number, wire_type, raw_data).
/// For varint (wire_type 0), data is the re-encoded varint bytes.
/// For length-delimited (wire_type 2), data is the payload bytes.
/// For fixed64 (wire_type 1), data is the 8 raw bytes.
fn parse_fields(data: &[u8]) -> Vec<(u32, u8, Vec<u8>)> {
    let mut fields = Vec::new();
    let mut pos = 0;
    while pos < data.len() {
        let (tag, n) = match decode_varint(&data[pos..]) {
            Some(v) => v,
            None => break,
        };
        pos += n;
        let field_number = (tag >> 3) as u32;
        let wire_type = (tag & 0x07) as u8;

        match wire_type {
            0 => {
                // Varint
                let (val, n) = match decode_varint(&data[pos..]) {
                    Some(v) => v,
                    None => break,
                };
                pos += n;
                fields.push((field_number, wire_type, val.to_le_bytes().to_vec()));
            }
            1 => {
                // Fixed64
                if pos + 8 > data.len() {
                    break;
                }
                fields.push((field_number, wire_type, data[pos..pos + 8].to_vec()));
                pos += 8;
            }
            2 => {
                // Length-delimited
                let (len, n) = match decode_varint(&data[pos..]) {
                    Some(v) => v,
                    None => break,
                };
                pos += n;
                let len = len as usize;
                if pos + len > data.len() {
                    break;
                }
                fields.push((field_number, wire_type, data[pos..pos + len].to_vec()));
                pos += len;
            }
            5 => {
                // Fixed32
                if pos + 4 > data.len() {
                    break;
                }
                fields.push((field_number, wire_type, data[pos..pos + 4].to_vec()));
                pos += 4;
            }
            _ => break,
        }
    }
    fields
}

fn get_varint_field(fields: &[(u32, u8, Vec<u8>)], field_num: u32) -> Option<u64> {
    for (num, wt, data) in fields {
        if *num == field_num && *wt == 0 {
            let mut val: u64 = 0;
            for (i, &b) in data.iter().enumerate().take(8) {
                val |= (b as u64) << (i * 8);
            }
            return Some(val);
        }
    }
    None
}

fn get_fixed32_field(fields: &[(u32, u8, Vec<u8>)], field_num: u32) -> Option<u32> {
    for (num, wt, data) in fields {
        if *num == field_num && *wt == 5 && data.len() >= 4 {
            return Some(u32::from_le_bytes([data[0], data[1], data[2], data[3]]));
        }
    }
    None
}

fn get_bytes_field<'a>(fields: &'a [(u32, u8, Vec<u8>)], field_num: u32) -> Option<&'a [u8]> {
    for (num, wt, data) in fields {
        if *num == field_num && *wt == 2 {
            return Some(data.as_slice());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// WebSocket frame layer (outer transport framing, NOT protobuf)
// ---------------------------------------------------------------------------

fn build_frame(frame_type: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![frame_type];
    out.extend(encode_varint(body.len() as u64));
    out.extend_from_slice(body);
    out
}

fn decode_frame(data: &[u8], pos: &mut usize) -> Option<(u8, Vec<u8>)> {
    if *pos >= data.len() {
        return None;
    }
    let frame_type = data[*pos];
    *pos += 1;
    let (len_val, n) = decode_varint(&data[*pos..])?;
    *pos += n;
    let len = len_val as usize;
    if *pos + len > data.len() {
        return None;
    }
    let payload = data[*pos..*pos + len].to_vec();
    *pos += len;
    Some((frame_type, payload))
}

fn decode_all_frames(data: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < data.len() {
        match decode_frame(data, &mut pos) {
            Some(v) => out.push(v),
            None => break,
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Frame builders
// ---------------------------------------------------------------------------

fn build_auth_frame(msg_id: u64, jwt: &str) -> Vec<u8> {
    let mut body = encode_field_varint(1, msg_id);
    body.extend(encode_field_string(3, jwt));
    build_frame(1, &body)
}

fn build_subscribe_frame(msg_id: u64) -> Vec<u8> {
    let mut body = encode_field_varint(1, msg_id);
    body.extend(encode_field_varint(3, 1));
    build_frame(2, &body)
}

fn build_payload_frame(msg_id: u64, qc_data: &[u8]) -> Vec<u8> {
    let mut body = encode_field_varint(1, msg_id);
    body.extend(encode_field_varint(2, now_millis()));
    body.extend(encode_field_varint(3, 1));
    body.extend(encode_field_bytes(5, &[0x02]));
    body.extend(encode_field_bytes(7, qc_data));
    build_frame(6, &body)
}

// ---------------------------------------------------------------------------
// QConnect message builders
// ---------------------------------------------------------------------------

/// Wraps a QConnect message (field 1 = message_type, field N = payload)
/// inside a field-3 container, as the protocol expects.
fn build_qconnect_message(message_type: u32, payload: &[u8]) -> Vec<u8> {
    let mut inner = encode_field_varint(1, message_type as u64);
    inner.extend(encode_field_bytes(message_type, payload));
    encode_field_bytes(3, &inner)
}

fn uuid_to_bytes(uuid_str: &str) -> Vec<u8> {
    uuid::Uuid::parse_str(uuid_str)
        .map(|u| u.as_bytes().to_vec())
        .unwrap_or_else(|_| uuid_str.as_bytes().to_vec())
}

fn build_device_info(device_uuid: &str, device_name: &str) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend(encode_field_bytes(1, &uuid_to_bytes(device_uuid))); // device_uuid
    out.extend(encode_field_string(2, device_name)); // friendly_name
    out.extend(encode_field_string(3, "QobuzD")); // brand
    out.extend(encode_field_string(4, "Linux")); // model
    out.extend(encode_field_string(5, device_uuid)); // serial_number
    out.extend(encode_field_varint(6, 5)); // type = COMPUTER(5)
    let mut caps = encode_field_varint(1, 1);
    caps.extend(encode_field_varint(2, 5));
    caps.extend(encode_field_varint(3, 2));
    out.extend(encode_field_bytes(7, &caps));
    out.extend(encode_field_string(8, "qobuzd-0.1.0")); // software_version
    out
}

/// CTRL_SRVR_JOIN_SESSION (61): controller asks server to create/join session.
fn msg_ctrl_join_session(device_uuid: &str, device_name: &str) -> Vec<u8> {
    let device_info = build_device_info(device_uuid, device_name);
    let payload = encode_field_bytes(2, &device_info);
    build_qconnect_message(61, &payload)
}

/// RNDR_SRVR_JOIN_SESSION (21): renderer joins an existing session.
fn msg_renderer_join_session(device_uuid: &str, device_name: &str, session_uuid: &[u8]) -> Vec<u8> {
    let device_info = build_device_info(device_uuid, device_name);
    let initial_state = build_renderer_state(1, 2, 0, 0, -1, -1); // stopped, buffer_state=OK(2)
    let mut payload = Vec::new();
    payload.extend(encode_field_bytes(1, session_uuid));
    payload.extend(encode_field_bytes(2, &device_info));
    payload.extend(encode_field_bytes(4, &initial_state));
    payload.extend(encode_field_varint(5, 1)); // is_active = true
    build_qconnect_message(21, &payload)
}

/// Build a RendererState protobuf.
/// buffer_state: 1=BUFFERING, 2=OK (per common.proto BufferState enum)
/// Encode a signed int32 as a protobuf varint field (sign-extended to 64 bits, matching proto int32 encoding).
fn encode_field_int32(field: u32, val: i32) -> Vec<u8> {
    let tag = (field as u64) << 3;
    let mut out = encode_varint(tag);
    // Protobuf int32 sign-extends to 64 bits: -1 becomes 0xFFFFFFFFFFFFFFFF (10-byte varint)
    out.extend(encode_varint(val as i64 as u64));
    out
}

fn build_renderer_state(
    playing_state: u64,
    buffer_state: u64,
    position_ms: u64,
    duration_ms: u64,
    queue_item_id: i32,
    next_queue_item_id: i32,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend(encode_field_varint(1, playing_state)); // field 1: playing_state
    out.extend(encode_field_varint(2, buffer_state)); // field 2: buffer_state
                                                      // field 3: current_position (PlaybackPosition: field 1=timestamp fixed64, field 2=value ms)
    let mut pos = Vec::new();
    pos.extend(encode_field_fixed64(1, now_millis())); // timestamp
    if playing_state != 1 || buffer_state != 1 {
        // Real app omits position_ms when STOPPED+BUFFERING(1)
        pos.extend(encode_field_varint(2, position_ms)); // value (ms)
    }
    out.extend(encode_field_bytes(3, &pos));
    if duration_ms > 0 {
        out.extend(encode_field_varint(4, duration_ms)); // field 4: duration (ms)
    }
    // field 5: queue_version (QueueVersion: field 1=major, field 2=minor)
    // mpv reference client sends QueueVersion(major=0, minor=0) — proto3 default encodes as empty submessage
    out.extend(encode_field_bytes(5, &[]));
    // field 6: current_queue_item_id — real app sends -1 when no track (never omits)
    out.extend(encode_field_int32(6, queue_item_id));
    // field 7: next_queue_item_id — real app sends -1 when no next track
    out.extend(encode_field_int32(7, next_queue_item_id));
    out
}

/// RNDR_SRVR_STATE_UPDATED (23): renderer reports its state.
fn msg_state_updated(
    playing_state: u64,
    buffer_state: u64,
    position_ms: u64,
    duration_ms: u64,
    queue_item_id: i32,
    next_queue_item_id: i32,
) -> Vec<u8> {
    let state = build_renderer_state(
        playing_state,
        buffer_state,
        position_ms,
        duration_ms,
        queue_item_id,
        next_queue_item_id,
    );
    let payload = encode_field_bytes(1, &state);
    build_qconnect_message(23, &payload)
}

/// Convert QConnect AudioQuality proto value to Qobuz API format_id.
/// Proto: 1=MP3, 2=CD, 3=HiRes96, 4=HiRes192, 5=HiRes192(max), 0/other=HiRes192 default
fn quality_to_format_id(quality: u32) -> u32 {
    match quality {
        1 => 5,      // MP3 320kbps
        2 => 6,      // FLAC CD 16-bit/44.1kHz
        3 => 7,      // FLAC Hi-Res 24-bit/96kHz
        4 | 5 => 27, // FLAC Hi-Res 24-bit/192kHz
        _ => 27,     // default to max quality
    }
}

/// RNDR_SRVR_RENDERER_ACTION (24): renderer reports a local user action.
/// ActionType: 0=UNKNOWN, 1=PREVIOUS, 2=NEXT, 3=REPEAT_OFF, 4=REPEAT_ONE, 5=REPEAT_ALL,
///             6=SHUFFLE_OFF, 7=SHUFFLE_ON, 8=SEEK
fn msg_renderer_action(action: u64, seek_position: Option<u32>) -> Vec<u8> {
    let mut payload = Vec::new();
    if let Some(pos) = seek_position {
        payload.extend(encode_field_varint(1, pos as u64)); // field 1: seek_position
    }
    payload.extend(encode_field_varint(2, action)); // field 2: action
    build_qconnect_message(24, &payload)
}

/// RNDR_SRVR_VOLUME_CHANGED (25): renderer reports volume.
fn msg_volume_changed(volume: u64) -> Vec<u8> {
    let payload = encode_field_varint(1, volume);
    build_qconnect_message(25, &payload)
}

/// RNDR_SRVR_MAX_AUDIO_QUALITY_CHANGED (28): renderer confirms quality setting.
/// networkType (field 2) is optional; when absent, quality applies generically.
fn msg_max_audio_quality_changed(quality: u64, network_type: Option<u64>) -> Vec<u8> {
    let mut payload = encode_field_varint(1, quality);
    if let Some(network_type) = network_type {
        payload.extend(encode_field_varint(2, network_type));
    }
    build_qconnect_message(28, &payload)
}

fn msg_file_audio_quality_changed(
    sampling_rate_hz: u64,
    bit_depth: u64,
    channels: u64,
    audio_quality: u64,
) -> Vec<u8> {
    let mut payload = encode_field_varint(1, sampling_rate_hz);
    payload.extend(encode_field_varint(2, bit_depth));
    payload.extend(encode_field_varint(3, channels));
    payload.extend(encode_field_varint(4, audio_quality));
    build_qconnect_message(26, &payload)
}

fn msg_device_audio_quality_changed(
    sampling_rate_hz: u64,
    bit_depth: u64,
    channels: u64,
) -> Vec<u8> {
    let mut payload = encode_field_varint(1, sampling_rate_hz);
    payload.extend(encode_field_varint(2, bit_depth));
    payload.extend(encode_field_varint(3, channels));
    build_qconnect_message(27, &payload)
}

fn quality_fallback_audio_params(quality: u32) -> (u32, u32, u32) {
    match quality {
        1 => (44100, 16, 2),  // MP3
        2 => (44100, 16, 2),  // CD
        3 => (96000, 24, 2),  // Hi-Res up to 96kHz
        4 | 5 => (192000, 24, 2), // Hi-Res up to 192/384kHz (use 192kHz fallback)
        _ => (44100, 16, 2),
    }
}

/// RNDR_SRVR_VOLUME_MUTED (29): renderer confirms mute state.
fn msg_volume_muted(muted: bool) -> Vec<u8> {
    let payload = encode_field_varint(1, if muted { 1 } else { 0 });
    build_qconnect_message(29, &payload)
}

// ---------------------------------------------------------------------------
// QConnect message parser — extracts messages from the frame layer
// ---------------------------------------------------------------------------

/// Extracts the QConnect Message from a data frame's body.
/// Frame body field 7 = qconnect container, which has field 3 = Message.
/// Returns (message_type, payload_for_that_type) pairs.
fn extract_qconnect_messages(frame_body: &[u8]) -> Vec<(u32, Vec<u8>)> {
    let mut result = Vec::new();
    let fields = parse_fields(frame_body);

    // Field 7 is the qconnect container
    for (fnum, wt, data) in &fields {
        if *fnum == 7 && *wt == 2 {
            // Inside field 7, field 3 is the serialized QConnect Message
            let container_fields = parse_fields(data);
            for (cfnum, cwt, cdata) in &container_fields {
                if *cfnum == 3 && *cwt == 2 {
                    // This is the QConnect Message
                    let msg_fields = parse_fields(cdata);
                    let msg_type = get_varint_field(&msg_fields, 1).unwrap_or(0) as u32;
                    // The payload is in the field whose number matches message_type
                    if let Some(payload) = get_bytes_field(&msg_fields, msg_type) {
                        result.push((msg_type, payload.to_vec()));
                    } else {
                        // Some messages have no sub-payload (just the type)
                        result.push((msg_type, Vec::new()));
                    }
                }
            }
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Parsed incoming commands
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum QConnectCommand {
    SetState {
        playing_state: Option<u32>, // None = not set (keep current), Some(1)=stopped, Some(2)=playing, Some(3)=paused
        position_ms: Option<u32>,   // None = field not present
        current_track: Option<TrackRef>,
        next_track: Option<TrackRef>,
        queue_version_major: u32,
    },
    SetVolume {
        volume: Option<u32>,
        delta: Option<i32>,
    },
    SetActive {
        active: bool,
    },
    SetLoopMode(u32),
    SetShuffleMode(u32),
    MuteVolume(bool),
    SetMaxAudioQuality(u32),
    Unknown(u32),
}

#[derive(Debug, Clone)]
pub struct TrackRef {
    pub queue_item_id: i32,
    pub track_id: i32,
}

fn parse_queue_track(data: &[u8]) -> TrackRef {
    let fields = parse_fields(data);
    let queue_item_id = get_varint_field(&fields, 1).unwrap_or(0) as i32;
    // track_id is fixed32 on the wire (not varint)
    let track_id = get_fixed32_field(&fields, 2).unwrap_or(0) as i32;
    TrackRef {
        queue_item_id,
        track_id,
    }
}

fn parse_incoming_commands(data: &[u8]) -> Vec<QConnectCommand> {
    let mut cmds = Vec::new();

    for (frame_type, frame_body) in decode_all_frames(data) {
        if frame_type != 6 {
            debug!(
                "[FRAME] type={} body={} bytes",
                frame_type,
                frame_body.len()
            );
            continue; // Only process data payload frames
        }

        for (msg_type, payload) in extract_qconnect_messages(&frame_body) {
            let cmd = match msg_type {
                // SRVR_RNDR_SET_STATE (41)
                41 => {
                    let fields = parse_fields(&payload);
                    let playing_state = get_varint_field(&fields, 1).map(|v| v as u32); // None = not present
                    let position_ms = get_varint_field(&fields, 2).map(|v| v as u32);
                    let queue_version_major = get_bytes_field(&fields, 3)
                        .map(|qv| {
                            let qvf = parse_fields(qv);
                            get_varint_field(&qvf, 1).unwrap_or(0) as u32
                        })
                        .unwrap_or(0);
                    let current_track = get_bytes_field(&fields, 4)
                        .map(parse_queue_track)
                        .and_then(|t| {
                            if t.track_id <= 0 || t.queue_item_id < 0 {
                                None
                            } else {
                                Some(t)
                            }
                        });
                    let next_track =
                        get_bytes_field(&fields, 5)
                            .map(parse_queue_track)
                            .and_then(|t| {
                                if t.track_id <= 0 || t.queue_item_id < 0 {
                                    None
                                } else {
                                    Some(t)
                                }
                            });

                    info!("[RECV] SET_STATE: playing_state={:?}, position_ms={:?}, current_track={:?}, next_track={:?}, queue_ver={}",
                        playing_state, position_ms, current_track, next_track, queue_version_major);

                    QConnectCommand::SetState {
                        playing_state,
                        position_ms,
                        current_track,
                        next_track,
                        queue_version_major,
                    }
                }
                // SRVR_RNDR_SET_VOLUME (42)
                42 => {
                    let fields = parse_fields(&payload);
                    let volume = get_varint_field(&fields, 1).map(|v| v as u32);
                    let delta = get_varint_field(&fields, 2).map(|v| v as i32);
                    QConnectCommand::SetVolume { volume, delta }
                }
                // SRVR_RNDR_SET_ACTIVE (43)
                43 => {
                    let fields = parse_fields(&payload);
                    let active = get_varint_field(&fields, 1).unwrap_or(0) != 0;
                    QConnectCommand::SetActive { active }
                }
                // SRVR_RNDR_SET_LOOP_MODE (45)
                45 => {
                    let fields = parse_fields(&payload);
                    let mode = get_varint_field(&fields, 1).unwrap_or(0) as u32;
                    QConnectCommand::SetLoopMode(mode)
                }
                // SRVR_RNDR_SET_SHUFFLE_MODE (46)
                46 => {
                    let fields = parse_fields(&payload);
                    let mode = get_varint_field(&fields, 1).unwrap_or(0) as u32;
                    QConnectCommand::SetShuffleMode(mode)
                }
                // SRVR_RNDR_MUTE_VOLUME (47)
                47 => {
                    let fields = parse_fields(&payload);
                    let muted = get_varint_field(&fields, 1).unwrap_or(0) != 0;
                    QConnectCommand::MuteVolume(muted)
                }
                // SRVR_RNDR_SET_MAX_AUDIO_QUALITY (44)
                44 => {
                    let fields = parse_fields(&payload);
                    let quality = get_varint_field(&fields, 1).unwrap_or(0) as u32;
                    QConnectCommand::SetMaxAudioQuality(quality)
                }
                other => {
                    info!(
                        "[RECV] Unknown msg type {}: payload {} bytes = {:02x?}",
                        other,
                        payload.len(),
                        &payload[..payload.len().min(64)]
                    );
                    QConnectCommand::Unknown(other)
                }
            };
            debug!("QConnect command: {:?}", cmd);
            cmds.push(cmd);
        }
    }
    cmds
}

// ---------------------------------------------------------------------------
// QConnect public API
// ---------------------------------------------------------------------------

pub struct QConnect {
    cmd_rx: mpsc::Receiver<QConnectCommand>,
}

impl QConnect {
    pub fn start(auth_token: String, device_uuid: String, device_name: String) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel::<QConnectCommand>(64);

        tokio::spawn(async move {
            qconnect_task(auth_token, device_uuid, device_name, cmd_tx).await;
        });

        Self { cmd_rx }
    }

    pub fn poll_command(&mut self) -> Option<QConnectCommand> {
        self.cmd_rx.try_recv().ok()
    }
}

// ---------------------------------------------------------------------------
// Connection logic
// ---------------------------------------------------------------------------

async fn qconnect_task(
    auth_token: String,
    device_uuid: String,
    device_name: String,
    cmd_tx: mpsc::Sender<QConnectCommand>,
) {
    let mut backoff = 5u64;
    loop {
        info!("QConnect: connecting...");
        match run_connection(&auth_token, &device_uuid, &device_name, &cmd_tx).await {
            Ok(()) => {
                info!("QConnect: disconnected cleanly");
                backoff = 5;
            }
            Err(e) => {
                error!("QConnect: error: {}", e);
            }
        }
        info!("QConnect: reconnecting in {}s", backoff);
        tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(120);
    }
}

async fn get_session_uuid(
    api: &QobuzApi,
    auth_token: &str,
    device_uuid: &str,
    device_name: &str,
) -> Result<Vec<u8>> {
    let token_resp = api.get_qws_token(auth_token).await?;
    let jwt = token_resp.jwt_qws.jwt;
    let endpoint = &token_resp.jwt_qws.endpoint;

    info!("QConnect ctrl: connecting to {}", endpoint);
    let (ws, _) = tokio_tungstenite::connect_async(endpoint).await?;
    let (mut tx, mut rx) = ws.split();

    // Auth
    tx.send(Message::Binary(build_auth_frame(1, &jwt).into()))
        .await?;
    if let Some(r) = rx.next().await {
        r?;
    }

    // Subscribe
    tx.send(Message::Binary(build_subscribe_frame(2).into()))
        .await?;
    if let Some(r) = rx.next().await {
        r?;
    }

    // Send ctrl_join_session
    let ctrl_join = build_payload_frame(3, &msg_ctrl_join_session(device_uuid, device_name));
    tx.send(Message::Binary(ctrl_join.into())).await?;

    // Wait for session UUID in response
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let remaining = deadline
            .checked_duration_since(std::time::Instant::now())
            .ok_or_else(|| anyhow::anyhow!("timeout waiting for session UUID"))?;

        match tokio::time::timeout(remaining, rx.next()).await {
            Ok(Some(Ok(Message::Binary(data)))) => {
                for (frame_type, frame_body) in decode_all_frames(&data) {
                    if frame_type != 6 {
                        continue;
                    }
                    // Look for session UUID in the SRVR_CTRL_SESSION_STATE (81) response
                    for (msg_type, payload) in extract_qconnect_messages(&frame_body) {
                        if msg_type == 81 {
                            // Session state — look for session UUID
                            let fields = parse_fields(&payload);
                            // Field 7 in session state might have device info
                            // Field 1 might be session UUID
                            if let Some(uuid_bytes) = get_bytes_field(&fields, 1) {
                                if uuid_bytes.len() == 16 {
                                    info!("Got session UUID from msg type 81");
                                    return Ok(uuid_bytes.to_vec());
                                }
                            }
                        }
                    }

                    // Fallback: scan frame field 7 deeply for any 16-byte UUID
                    let frame_fields = parse_fields(&frame_body);
                    if let Some(f7) = get_bytes_field(&frame_fields, 7) {
                        if let Some(uuid) = find_session_uuid(f7) {
                            info!("Got session UUID from deep scan");
                            return Ok(uuid);
                        }
                    }
                }
            }
            Ok(Some(Ok(Message::Ping(data)))) => {
                tx.send(Message::Pong(data)).await?;
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(e))) => bail!("ctrl connection error: {}", e),
            _ => bail!("timeout waiting for session UUID"),
        }
    }
}

/// Recursively search for a 16-byte blob that looks like a session UUID.
fn find_session_uuid(data: &[u8]) -> Option<Vec<u8>> {
    let fields = parse_fields(data);
    if let Some(candidate) = get_bytes_field(&fields, 1) {
        // Prefer structures that look like SessionState payloads:
        // field 1 = 16-byte UUID, field 2 = enum/int varint.
        if candidate.len() == 16 && get_varint_field(&fields, 2).is_some() {
            return Some(candidate.to_vec());
        }
    }

    for (_, wt, field_data) in &fields {
        if *wt == 2 {
            if let Some(found) = find_session_uuid(field_data) {
                return Some(found);
            }
        }
    }
    None
}

async fn run_connection(
    auth_token: &str,
    device_uuid: &str,
    device_name: &str,
    cmd_tx: &mpsc::Sender<QConnectCommand>,
) -> Result<()> {
    let config = Config::load().map_err(|e| anyhow::anyhow!("{}", e))?;
    let api = QobuzApi::new(&config);

    // 1. Get session UUID via ctrl connection
    info!("QConnect: getting session UUID...");
    let session_uuid = get_session_uuid(&api, auth_token, device_uuid, device_name).await?;
    info!("QConnect: got session UUID ({} bytes)", session_uuid.len());

    // 2. Open renderer connection
    let token_resp = api.get_qws_token(auth_token).await?;
    let jwt = token_resp.jwt_qws.jwt;
    let endpoint = &token_resp.jwt_qws.endpoint;

    info!("QConnect renderer: connecting to {}", endpoint);
    let (ws, _) = tokio_tungstenite::connect_async(endpoint).await?;
    let (mut ws_tx, mut ws_rx) = ws.split();

    let mut msg_id: u64 = 1;

    // Auth
    ws_tx
        .send(Message::Binary(build_auth_frame(msg_id, &jwt).into()))
        .await?;
    msg_id += 1;
    if let Some(r) = ws_rx.next().await {
        r?;
    }

    // Subscribe
    ws_tx
        .send(Message::Binary(build_subscribe_frame(msg_id).into()))
        .await?;
    msg_id += 1;
    if let Some(r) = ws_rx.next().await {
        r?;
    }

    // Join session as renderer
    let join_msg = msg_renderer_join_session(device_uuid, device_name, &session_uuid);
    ws_tx
        .send(Message::Binary(
            build_payload_frame(msg_id, &join_msg).into(),
        ))
        .await?;
    msg_id += 1;

    // Read join response
    for _ in 0..5 {
        match tokio::time::timeout(std::time::Duration::from_secs(5), ws_rx.next()).await {
            Ok(Some(Ok(Message::Binary(data)))) => {
                for (frame_type, frame_body) in decode_all_frames(&data) {
                    if frame_type != 6 {
                        continue;
                    }
                    for (mt, payload) in extract_qconnect_messages(&frame_body) {
                        if mt == 1 {
                            // Error
                            let fields = parse_fields(&payload);
                            let code = get_bytes_field(&fields, 1)
                                .and_then(|b| std::str::from_utf8(b).ok())
                                .unwrap_or("?");
                            let message = get_bytes_field(&fields, 2)
                                .and_then(|b| std::str::from_utf8(b).ok())
                                .unwrap_or("?");
                            bail!("renderer join rejected: {} — {}", code, message);
                        }
                        if mt == 43 {
                            info!("QConnect: renderer joined (SET_ACTIVE received)");
                        }
                    }
                }
                break;
            }
            Ok(Some(Ok(Message::Ping(data)))) => {
                ws_tx.send(Message::Pong(data)).await?;
            }
            Ok(Some(Ok(_))) => break,
            Ok(Some(Err(e))) => bail!("WS error on join: {}", e),
            _ => break,
        }
    }
    info!("QConnect: joined session as renderer");

    // Send initial state (stopped, buffer_state=OK) and volume
    {
        let state_msg = msg_state_updated(1, 2, 0, 0, -1, -1);
        ws_tx
            .send(Message::Binary(
                build_payload_frame(msg_id, &state_msg).into(),
            ))
            .await?;
        msg_id += 1;

        let vol_msg = msg_volume_changed(100);
        ws_tx
            .send(Message::Binary(
                build_payload_frame(msg_id, &vol_msg).into(),
            ))
            .await?;
        msg_id += 1;
    }

    // Create audio player
    let player = AudioPlayer::new();

    // Local state tracking (optimistic — reflects what we've been told to do)
    let mut current_playing_state: u64 = 1; // 1=stopped, 2=playing, 3=paused
    let mut current_queue_item_id: i32 = -1;
    let mut current_next_queue_item_id: i32 = -1;
    let mut current_position_ms: u64 = 0;
    let mut current_duration_ms: u64 = 0;
    let mut current_buffer_state: u64 = 2; // 2=OK per proto
    let mut volume: u8 = 100;
    let mut muted = false;
    let mut pre_mute_volume: u8 = 100;
    let mut max_audio_quality: u32 = 4; // proto quality value 4 = Hi-Res 192
    let mut current_track_id: i32 = 0; // track_id of currently playing track
    let mut last_play_command_at: std::time::Instant = std::time::Instant::now();
    let mut has_seen_position_progress = false; // true once we've seen pos > 0 after a Play
    let mut track_ended = false; // true when player finishes track naturally
    let mut ignore_nonzero_seek_until: Option<std::time::Instant> = None;

    // Helper macro: send a state update
    macro_rules! send_state {
        ($ws_tx:expr, $msg_id:expr) => {{
            debug!(
                "[SEND] StateUpdated: playing={} buffer={} pos={}ms dur={}ms qi={} nqi={}",
                current_playing_state,
                current_buffer_state,
                current_position_ms,
                current_duration_ms,
                current_queue_item_id,
                current_next_queue_item_id
            );
            let sm = msg_state_updated(
                current_playing_state,
                current_buffer_state,
                current_position_ms,
                current_duration_ms,
                current_queue_item_id,
                current_next_queue_item_id,
            );
            $ws_tx
                .send(Message::Binary(build_payload_frame($msg_id, &sm).into()))
                .await?;
            $msg_id += 1;
        }};
    }

    info!("QConnect: entering main loop");
    let mut position_ticker = tokio::time::interval(std::time::Duration::from_millis(500));
    position_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = position_ticker.tick() => {
                if current_playing_state == 2 {
                    let status = player.status();
                    let elapsed_since_play = last_play_command_at.elapsed();

                    if status.state == PlayerState::Stopped
                        && has_seen_position_progress
                        && elapsed_since_play > std::time::Duration::from_secs(3)
                    {
                        if !track_ended {
                            // Track ended naturally — send final position then request next track
                            info!("[TICK] Track ended naturally, sending ACTION_TYPE_NEXT");
                            track_ended = true;
                            current_position_ms = current_duration_ms;
                            send_state!(ws_tx, msg_id);

                            // Tell server to advance to next track (ACTION_TYPE_NEXT = 2)
                            let action_msg = msg_renderer_action(2, None);
                            ws_tx.send(Message::Binary(build_payload_frame(msg_id, &action_msg).into())).await?;
                            msg_id += 1;
                        }
                        // Don't spam — wait for server to send new SET_STATE
                    } else if status.state == PlayerState::Stopped {
                        debug!("[TICK] Player stopped but grace period (elapsed={:?}, progress={}), ignoring",
                            elapsed_since_play, has_seen_position_progress);
                    } else {
                        let new_pos = status.position_ms;
                        if new_pos > 0 && !has_seen_position_progress {
                            has_seen_position_progress = true;
                            info!("[TICK] First position progress: {}ms", new_pos);
                        }
                        if new_pos != current_position_ms {
                            current_position_ms = new_pos;
                            send_state!(ws_tx, msg_id);
                        }
                    }
                }
            }
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        let cmds = parse_incoming_commands(&data);
                        for cmd in cmds {
                            let _ = cmd_tx.try_send(cmd.clone());

                            match &cmd {
                                QConnectCommand::SetState {
                                    playing_state,
                                    position_ms,
                                    current_track,
                                    next_track,
                                    queue_version_major,
                                } => {
                                    info!("[STATE] SET_STATE: playing_state={:?} current_track={:?} next_track={:?} pos={}",
                                        playing_state, current_track.as_ref().map(|t| t.track_id),
                                        next_track.as_ref().map(|t| t.track_id), position_ms.unwrap_or(0));

                                    let requested_pos = position_ms.map(|p| p as u64);

                                    let seek_only_state = playing_state.is_none()
                                        && current_track.is_none()
                                        && next_track.is_none()
                                        && *queue_version_major == 0
                                        && requested_pos.is_some();

                                    if seek_only_state {
                                        let target_pos = requested_pos.unwrap_or(0);
                                        let status = player.status();
                                        let current_player_pos = status.position_ms;
                                        let mut should_seek = target_pos == 0
                                            || target_pos.abs_diff(current_player_pos) > 350;
                                        let suppress_nonzero_seek = target_pos > 0
                                            && ignore_nonzero_seek_until
                                                .map(|deadline| std::time::Instant::now() < deadline)
                                                .unwrap_or(false);
                                        if suppress_nonzero_seek {
                                            should_seek = false;
                                            info!(
                                                "[STATE] Ignoring non-zero seek {}ms during settle window",
                                                target_pos
                                            );
                                        }

                                        info!(
                                            "[STATE] seek-only command: target={}ms local={}ms state={:?} track={} should_seek={}",
                                            target_pos,
                                            current_player_pos,
                                            status.state,
                                            status.track_id,
                                            should_seek
                                        );

                                        if should_seek {
                                            info!(
                                                "[STATE] Applying seek to {}ms (local={}ms)",
                                                target_pos, current_player_pos
                                            );
                                            player.send(PlayerCommand::Seek(target_pos));
                                            track_ended = false;
                                            if target_pos == 0 {
                                                ignore_nonzero_seek_until = Some(
                                                    std::time::Instant::now()
                                                        + std::time::Duration::from_secs(2),
                                                );
                                            }
                                        }

                                        current_position_ms = target_pos;
                                        send_state!(ws_tx, msg_id);
                                        continue;
                                    }

                                    // 1. Store next_track metadata
                                    if let Some(nt) = next_track {
                                        current_next_queue_item_id = nt.queue_item_id;
                                    }

                                    // 2. Load new current_track if present and different
                                    let mut loaded_new_track = false;
                                    if let Some(track) = current_track {
                                        if track.track_id != current_track_id || track.queue_item_id != current_queue_item_id {
                                            info!("[STATE] Loading new track {} (qi={})", track.track_id, track.queue_item_id);
                                            current_track_id = track.track_id;
                                            current_queue_item_id = track.queue_item_id;
                                            current_playing_state = 2;
                                            current_buffer_state = 1; // BUFFERING
                                            current_position_ms = requested_pos.unwrap_or(0);
                                            current_duration_ms = 0;
                                            last_play_command_at = std::time::Instant::now();
                                            has_seen_position_progress = false;
                                            track_ended = false;
                                            ignore_nonzero_seek_until = None;
                                            send_state!(ws_tx, msg_id);

                                            let track_id_str = track.track_id.to_string();
                                            let format_id = quality_to_format_id(max_audio_quality);
                                            let duration_ms = match api.get_track(auth_token, &track_id_str).await {
                                                Ok(t) => t.duration.unwrap_or(0) as u64 * 1000,
                                                Err(e) => { warn!("get_track failed: {}", e); 0 }
                                            };
                                            current_duration_ms = duration_ms;
                                            match api.get_track_stream(auth_token, &track_id_str, format_id).await {
                                                Ok(stream) => {
                                                    let (player_stream, stream_sr, stream_bits, stream_ch) = match stream {
                                                        TrackStream::DirectUrl {
                                                            url,
                                                            sampling_rate_hz,
                                                            bit_depth,
                                                            channels,
                                                        } => {
                                                            info!("[STATE] Got direct stream URL (duration={}ms)", duration_ms);
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
                                                            info!(
                                                                "[STATE] Got segmented stream (segments={}, duration={}ms)",
                                                                n_segments, duration_ms
                                                            );
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
                                                    player.send(PlayerCommand::Play {
                                                        stream: player_stream,
                                                        track_id: track.track_id,
                                                        queue_item_id: track.queue_item_id,
                                                        duration_ms,
                                                        start_position_ms: requested_pos.unwrap_or(0),
                                                    });
                                                    let (fallback_sr, fallback_bits, fallback_ch) =
                                                        quality_fallback_audio_params(max_audio_quality);
                                                    let sr = stream_sr.unwrap_or(fallback_sr).max(1);
                                                    let bits = stream_bits.unwrap_or(fallback_bits).max(1);
                                                    let ch = stream_ch.unwrap_or(fallback_ch).max(1);
                                                    let file_msg = msg_file_audio_quality_changed(
                                                        sr as u64,
                                                        bits as u64,
                                                        ch as u64,
                                                        max_audio_quality as u64,
                                                    );
                                                    ws_tx.send(Message::Binary(build_payload_frame(msg_id, &file_msg).into())).await?;
                                                    msg_id += 1;
                                                    let dev_msg = msg_device_audio_quality_changed(
                                                        sr as u64,
                                                        bits as u64,
                                                        ch as u64,
                                                    );
                                                    ws_tx.send(Message::Binary(build_payload_frame(msg_id, &dev_msg).into())).await?;
                                                    msg_id += 1;
                                                    current_buffer_state = 2; // OK
                                                }
                                                Err(e) => {
                                                    error!("[STATE] Failed to get stream URL: {}", e);
                                                    current_playing_state = 1;
                                                    current_buffer_state = 2; // OK
                                                }
                                            }
                                            loaded_new_track = true;
                                        }
                                    }

                                    // 3. Apply playing_state if present (and we didn't just load a new track)
                                    if let Some(ps) = playing_state {
                                        if !loaded_new_track {
                                            match ps {
                                                2 => {
                                                    let status = player.status();
                                                    let should_restart_same_track = current_track.is_some()
                                                        && (track_ended || status.state == PlayerState::Stopped);

                                                    if should_restart_same_track {
                                                        let restart_pos = requested_pos.unwrap_or(0);
                                                        info!(
                                                            "[STATE] Restarting current track from {}ms (ended={} player_state={:?})",
                                                            restart_pos,
                                                            track_ended,
                                                            status.state
                                                        );
                                                        player.send(PlayerCommand::Seek(restart_pos));
                                                        current_playing_state = 2;
                                                        current_position_ms = restart_pos;
                                                        track_ended = false;
                                                    } else if current_playing_state == 3 {
                                                        info!("[STATE] Resuming playback");
                                                        player.send(PlayerCommand::Resume);
                                                        current_playing_state = 2;
                                                        track_ended = false;
                                                    } else if current_playing_state != 2 {
                                                        info!("[STATE] Play requested but no new track, state={}", current_playing_state);
                                                        current_playing_state = 2;
                                                    }
                                                }
                                                3 => {
                                                    info!("[STATE] Pausing playback");
                                                    player.send(PlayerCommand::Pause);
                                                    current_playing_state = 3;
                                                    if let Some(pos) = requested_pos {
                                                        current_position_ms = pos;
                                                    } else {
                                                        current_position_ms = player.status().position_ms;
                                                    }
                                                }
                                                1 => {
                                                    info!("[STATE] Stopping playback");
                                                    player.send(PlayerCommand::Stop);
                                                    current_playing_state = 1;
                                                    current_position_ms = 0;
                                                    current_queue_item_id = -1;
                                                    current_next_queue_item_id = -1;
                                                    current_track_id = 0;
                                                    track_ended = false;
                                                }
                                                _ => {}
                                            }
                                        }
                                    }

                                    // 4. Apply seek position if provided and not loading new track
                                    let is_pause = matches!(playing_state, Some(3));
                                    let position_control_state = *queue_version_major == 0
                                        && current_track.is_none()
                                        && next_track.is_none()
                                        && requested_pos.is_some();
                                    if !loaded_new_track && !is_pause && position_control_state {
                                        let requested = requested_pos.unwrap_or(0);
                                        let status = player.status();
                                        let local = status.position_ms;
                                        let mut should_seek = requested == 0
                                            || requested.abs_diff(local) > 350;
                                        let suppress_nonzero_seek = requested > 0
                                            && ignore_nonzero_seek_until
                                                .map(|deadline| std::time::Instant::now() < deadline)
                                                .unwrap_or(false);
                                        if suppress_nonzero_seek {
                                            should_seek = false;
                                            info!(
                                                "[STATE] Ignoring non-zero position control {}ms during settle window",
                                                requested
                                            );
                                        }
                                        info!(
                                            "[STATE] position-control command: playing_state={:?} target={}ms local={}ms state={:?} track={} should_seek={}",
                                            playing_state,
                                            requested,
                                            local,
                                            status.state,
                                            status.track_id,
                                            should_seek
                                        );

                                        if should_seek {
                                            info!(
                                                "[STATE] Position jump detected, seeking to {}ms (local={}ms)",
                                                requested, local
                                            );
                                            player.send(PlayerCommand::Seek(requested));
                                            track_ended = false;
                                            if requested == 0 {
                                                ignore_nonzero_seek_until = Some(
                                                    std::time::Instant::now()
                                                        + std::time::Duration::from_secs(2),
                                                );
                                            }
                                        }
                                        current_position_ms = requested;
                                    } else if !loaded_new_track && !is_pause {
                                        if let Some(pos) = requested_pos {
                                            current_position_ms = pos;
                                        }
                                    }

                                    // 5. Always send state update (like reference implementation)
                                    send_state!(ws_tx, msg_id);
                                }

                                QConnectCommand::SetVolume { volume: vol, delta } => {
                                    let new_vol = if let Some(v) = vol {
                                        (*v).min(100) as u8
                                    } else if let Some(d) = delta {
                                        (volume as i32 + d).clamp(0, 100) as u8
                                    } else {
                                        volume
                                    };
                                    info!("Setting volume to {}", new_vol);
                                    volume = new_vol;
                                    if muted && new_vol > 0 { muted = false; }
                                    player.send(PlayerCommand::SetVolume(new_vol));
                                    let resp = msg_volume_changed(new_vol as u64);
                                    ws_tx.send(Message::Binary(build_payload_frame(msg_id, &resp).into())).await?;
                                    msg_id += 1;
                                }

                                QConnectCommand::SetActive { active } => {
                                    info!("SetActive: {}", active);
                                    if !*active {
                                        player.send(PlayerCommand::Stop);
                                        current_playing_state = 1;
                                        current_buffer_state = 2; // OK
                                        current_position_ms = 0;
                                        current_queue_item_id = -1;
                                        current_next_queue_item_id = -1;
                                        current_track_id = 0;
                                        send_state!(ws_tx, msg_id);
                                    }
                                }

                                QConnectCommand::MuteVolume(mute) => {
                                    info!("MuteVolume: {}", mute);
                                    if *mute {
                                        pre_mute_volume = volume;
                                        volume = 0;
                                        muted = true;
                                    } else {
                                        volume = pre_mute_volume;
                                        muted = false;
                                    }
                                    player.send(PlayerCommand::SetVolume(volume));
                                    let resp = msg_volume_muted(*mute);
                                    ws_tx.send(Message::Binary(build_payload_frame(msg_id, &resp).into())).await?;
                                    msg_id += 1;
                                }

                                QConnectCommand::SetMaxAudioQuality(quality) => {
                                    let format_id = quality_to_format_id(*quality);
                                    info!("SetMaxAudioQuality: {} (format_id={})", quality, format_id);
                                    max_audio_quality = *quality;

                                    // Confirm quality change to server
                                    let resp = msg_max_audio_quality_changed(*quality as u64, None);
                                    ws_tx.send(Message::Binary(build_payload_frame(msg_id, &resp).into())).await?;
                                    msg_id += 1;

                                    // If currently playing, restart at new quality
                                    if current_playing_state == 2 && current_track_id != 0 {
                                        let restart_pos = player.status().position_ms;
                                        info!("Restarting track {} at new quality format_id={}", current_track_id, format_id);
                                        current_buffer_state = 1; // BUFFERING
                                        current_position_ms = restart_pos;
                                        send_state!(ws_tx, msg_id);

                                        let track_id_str = current_track_id.to_string();
                                        let duration_ms = match api.get_track(auth_token, &track_id_str).await {
                                            Ok(t) => t.duration.unwrap_or(0) as u64 * 1000,
                                            Err(e) => { warn!("get_track failed: {}", e); current_duration_ms }
                                        };
                                        current_duration_ms = duration_ms;
                                        match api.get_track_stream(auth_token, &track_id_str, format_id).await {
                                            Ok(stream) => {
                                                let (player_stream, stream_sr, stream_bits, stream_ch) = match stream {
                                                    TrackStream::DirectUrl {
                                                        url,
                                                        sampling_rate_hz,
                                                        bit_depth,
                                                        channels,
                                                    } => {
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
                                                player.send(PlayerCommand::Play {
                                                    stream: player_stream,
                                                    track_id: current_track_id,
                                                    queue_item_id: current_queue_item_id,
                                                    duration_ms,
                                                    start_position_ms: restart_pos,
                                                });
                                                let (fallback_sr, fallback_bits, fallback_ch) =
                                                    quality_fallback_audio_params(*quality);
                                                let sr = stream_sr.unwrap_or(fallback_sr).max(1);
                                                let bits = stream_bits.unwrap_or(fallback_bits).max(1);
                                                let ch = stream_ch.unwrap_or(fallback_ch).max(1);
                                                let file_msg = msg_file_audio_quality_changed(
                                                    sr as u64,
                                                    bits as u64,
                                                    ch as u64,
                                                    *quality as u64,
                                                );
                                                ws_tx.send(Message::Binary(build_payload_frame(msg_id, &file_msg).into())).await?;
                                                msg_id += 1;
                                                let dev_msg = msg_device_audio_quality_changed(
                                                    sr as u64,
                                                    bits as u64,
                                                    ch as u64,
                                                );
                                                ws_tx.send(Message::Binary(build_payload_frame(msg_id, &dev_msg).into())).await?;
                                                msg_id += 1;
                                                // Re-emit quality confirmation after successful restart
                                                // so controllers observing the currently active stream update promptly.
                                                let confirm = msg_max_audio_quality_changed(*quality as u64, None);
                                                ws_tx.send(Message::Binary(build_payload_frame(msg_id, &confirm).into())).await?;
                                                msg_id += 1;
                                                current_buffer_state = 2; // OK(2)
                                                info!("Restarted at format_id={}", format_id);
                                            }
                                            Err(e) => {
                                                error!("Failed to get stream URL for quality change: {}", e);
                                                current_playing_state = 1;
                                                current_buffer_state = 2; // OK
                                            }
                                        }
                                        send_state!(ws_tx, msg_id);
                                    }
                                }

                                QConnectCommand::SetLoopMode(mode) => {
                                    info!("SetLoopMode: {}", mode);
                                    let _ = mode;
                                    // No response message — renderer stores setting, server notifies controllers directly
                                }

                                QConnectCommand::SetShuffleMode(mode) => {
                                    info!("SetShuffleMode: {}", mode);
                                    let _ = mode;
                                    // No response message — renderer stores setting, server notifies controllers directly
                                }

                                QConnectCommand::Unknown(_) => {}
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        ws_tx.send(Message::Pong(data)).await?;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        bail!("WebSocket closed");
                    }
                    Some(Err(e)) => {
                        bail!("WebSocket error: {}", e);
                    }
                    _ => {}
                }
            }
        }
    }
}
