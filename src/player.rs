use std::collections::VecDeque;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use aes::Aes128;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ctr::cipher::{KeyIvInit, StreamCipher};
use ctr::Ctr128BE;
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::DecoderOptions;
use symphonia::core::formats::{FormatOptions, SeekMode, SeekTo};
use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;
use symphonia::core::units::Time;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

#[derive(Debug)]
pub enum PlayerCommand {
    Play {
        stream: StreamSource,
        track_id: i32,
        queue_item_id: i32,
        duration_ms: u64,
        start_position_ms: u64,
    },
    Resume,
    Pause,
    Stop,
    Seek(u64),
    SetVolume(u8),
}

#[derive(Debug, Clone)]
pub enum StreamSource {
    DirectUrl(String),
    Segmented {
        url_template: String,
        n_segments: u32,
        encryption_key_hex: Option<String>,
    },
}

struct SampleChunk {
    generation: u64,
    frame_count: u64,
    samples: Vec<f32>,
}

struct PersistentAudioOutput {
    sample_rate: u32,
    channels: usize,
    sample_tx: std::sync::mpsc::SyncSender<SampleChunk>,
    _stream: cpal::Stream,
}

enum OutputThreadCommand {
    Configure {
        sample_rate: u32,
        channels: usize,
        ack_tx: std::sync::mpsc::SyncSender<Result<(), String>>,
    },
    Samples(SampleChunk),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PlayerState {
    Stopped,
    Playing,
    Paused,
}

pub struct PlayerStatus {
    pub state: PlayerState,
    pub position_ms: u64,
    pub duration_ms: u64,
    pub track_id: i32,
    pub queue_item_id: i32,
    pub volume: u8,
}

struct SharedState {
    playing: AtomicBool,
    paused: AtomicBool,
    stop_signal: AtomicBool,
    generation: AtomicU64, // incremented on each Play, used to avoid old threads clobbering state
    playback_base_position_ms: AtomicU64,
    played_frames: AtomicU64,
    queued_frames: AtomicU64,
    position_ms: AtomicU64,
    duration_ms: AtomicU64,
    volume: AtomicU8,
    track_id: AtomicI32,
    queue_item_id: AtomicI32,
    output_tx: std::sync::mpsc::SyncSender<OutputThreadCommand>,
}

pub struct AudioPlayer {
    cmd_tx: mpsc::UnboundedSender<PlayerCommand>,
    shared: Arc<SharedState>,
}

#[derive(Debug, Clone)]
struct PlaybackRequest {
    stream: StreamSource,
    track_id: i32,
    queue_item_id: i32,
    duration_ms: u64,
    start_position_ms: u64,
}

fn start_playback(shared: &Arc<SharedState>, req: &PlaybackRequest) {
    info!(
        "Player start request: track_id={} qi={} start={}ms duration={}ms",
        req.track_id, req.queue_item_id, req.start_position_ms, req.duration_ms
    );

    shared.stop_signal.store(true, Ordering::SeqCst);
    std::thread::sleep(std::time::Duration::from_millis(100));
    shared.stop_signal.store(false, Ordering::SeqCst);

    let generation = shared.generation.fetch_add(1, Ordering::SeqCst) + 1;
    shared.paused.store(false, Ordering::SeqCst);
    shared
        .playback_base_position_ms
        .store(req.start_position_ms, Ordering::SeqCst);
    shared.played_frames.store(0, Ordering::SeqCst);
    shared.queued_frames.store(0, Ordering::SeqCst);
    shared
        .position_ms
        .store(req.start_position_ms, Ordering::SeqCst);
    shared.duration_ms.store(req.duration_ms, Ordering::SeqCst);
    shared.track_id.store(req.track_id, Ordering::SeqCst);
    shared
        .queue_item_id
        .store(req.queue_item_id, Ordering::SeqCst);
    shared.playing.store(true, Ordering::SeqCst);

    let shared_play = shared.clone();
    let stream = req.stream.clone();
    let start_position_ms = req.start_position_ms;
    std::thread::spawn(move || {
        if let Err(e) = play_stream(&stream, shared_play, generation, start_position_ms) {
            error!("Playback error: {}", e);
        }
    });
}

fn atomic_saturating_sub_u64(value: &AtomicU64, amount: u64) {
    loop {
        let current = value.load(Ordering::Relaxed);
        let next = current.saturating_sub(amount);
        if value
            .compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }
    }
}

fn ensure_audio_output(
    shared: &Arc<SharedState>,
    sample_rate: u32,
    channels: usize,
) -> anyhow::Result<()> {
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    shared
        .output_tx
        .send(OutputThreadCommand::Configure {
            sample_rate,
            channels,
            ack_tx,
        })
        .map_err(|_| anyhow::anyhow!("audio output thread disconnected"))?;

    match ack_rx.recv_timeout(std::time::Duration::from_secs(5)) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(msg)) => Err(anyhow::anyhow!(msg)),
        Err(_) => Err(anyhow::anyhow!(
            "timed out while waiting for audio output configuration"
        )),
    }
}

fn queue_samples_for_playback(
    shared: &Arc<SharedState>,
    generation: u64,
    channels: usize,
    samples: Vec<f32>,
) -> bool {
    let frame_count = (samples.len() / channels) as u64;
    if frame_count == 0 {
        return true;
    }

    shared
        .queued_frames
        .fetch_add(frame_count, Ordering::Relaxed);
    let mut pending = Some(SampleChunk {
        generation,
        frame_count,
        samples,
    });

    loop {
        if shared.generation.load(Ordering::SeqCst) != generation
            || shared.stop_signal.load(Ordering::Relaxed)
        {
            atomic_saturating_sub_u64(&shared.queued_frames, frame_count);
            return false;
        }

        match shared.output_tx.try_send(OutputThreadCommand::Samples(
            pending.take().expect("pending chunk missing"),
        )) {
            Ok(()) => return true,
            Err(std::sync::mpsc::TrySendError::Full(OutputThreadCommand::Samples(chunk))) => {
                pending = Some(chunk);
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            Err(std::sync::mpsc::TrySendError::Full(OutputThreadCommand::Configure { .. })) => {
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                atomic_saturating_sub_u64(&shared.queued_frames, frame_count);
                return false;
            }
        }
    }
}

fn open_output_stream(
    shared: Arc<SharedState>,
    sample_rate: u32,
    channels: usize,
) -> anyhow::Result<PersistentAudioOutput> {
    let host = cpal::default_host();
    let device = host
        .default_output_device()
        .ok_or_else(|| anyhow::anyhow!("no audio output device"))?;

    info!(
        "Opening audio output at {}Hz, {}ch on {}",
        sample_rate,
        channels,
        device.name().unwrap_or_default()
    );

    let config = cpal::StreamConfig {
        channels: channels as u16,
        sample_rate: cpal::SampleRate(sample_rate),
        buffer_size: cpal::BufferSize::Default,
    };

    let (sample_tx, sample_rx) = std::sync::mpsc::sync_channel::<SampleChunk>(8);
    let shared_out = shared.clone();
    let channel_count = channels;
    let sample_rate_u64 = sample_rate as u64;
    let mut ring_chunk: Option<SampleChunk> = None;
    let mut ring_pos = 0usize;

    let stream = device.build_output_stream(
        &config,
        move |out: &mut [f32], _: &cpal::OutputCallbackInfo| {
            let vol = shared_out.volume.load(Ordering::Relaxed) as f32 / 100.0;
            let paused = shared_out.paused.load(Ordering::Relaxed);
            let active_generation = shared_out.generation.load(Ordering::Relaxed);
            let mut frames_consumed = 0u64;

            for frame in out.chunks_mut(channel_count) {
                if paused {
                    frame.fill(0.0);
                    continue;
                }

                loop {
                    let needs_chunk = match ring_chunk.as_ref() {
                        Some(chunk) => {
                            if chunk.generation != active_generation {
                                let remaining_samples =
                                    chunk.samples.len().saturating_sub(ring_pos);
                                let remaining_frames = (remaining_samples / channel_count) as u64;
                                if remaining_frames > 0 {
                                    atomic_saturating_sub_u64(
                                        &shared_out.queued_frames,
                                        remaining_frames,
                                    );
                                }
                                ring_chunk = None;
                                ring_pos = 0;
                                true
                            } else {
                                ring_pos >= chunk.samples.len()
                            }
                        }
                        None => true,
                    };

                    if !needs_chunk {
                        break;
                    }

                    match sample_rx.try_recv() {
                        Ok(chunk) => {
                            ring_chunk = Some(chunk);
                            ring_pos = 0;
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty)
                        | Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            ring_chunk = None;
                            ring_pos = 0;
                            break;
                        }
                    }
                }

                if let Some(chunk) = ring_chunk.as_ref() {
                    if ring_pos + channel_count <= chunk.samples.len() {
                        for sample in frame.iter_mut() {
                            *sample = chunk.samples[ring_pos] * vol;
                            ring_pos += 1;
                        }
                        frames_consumed += 1;
                    } else {
                        ring_chunk = None;
                        ring_pos = 0;
                        frame.fill(0.0);
                    }
                } else {
                    frame.fill(0.0);
                }
            }

            if frames_consumed > 0 {
                let total_played = shared_out
                    .played_frames
                    .fetch_add(frames_consumed, Ordering::Relaxed)
                    + frames_consumed;
                let played_ms = total_played.saturating_mul(1000) / sample_rate_u64.max(1);
                let base_ms = shared_out.playback_base_position_ms.load(Ordering::Relaxed);
                let pos_ms = base_ms.saturating_add(played_ms);
                shared_out.position_ms.store(pos_ms, Ordering::Relaxed);
                atomic_saturating_sub_u64(&shared_out.queued_frames, frames_consumed);
            }
        },
        |err| error!("cpal error: {}", err),
        None,
    )?;

    stream.play()?;

    Ok(PersistentAudioOutput {
        sample_rate,
        channels,
        sample_tx,
        _stream: stream,
    })
}

fn output_thread_loop(
    rx: std::sync::mpsc::Receiver<OutputThreadCommand>,
    shared: Arc<SharedState>,
) {
    let mut active_output: Option<PersistentAudioOutput> = None;

    while let Ok(cmd) = rx.recv() {
        match cmd {
            OutputThreadCommand::Configure {
                sample_rate,
                channels,
                ack_tx,
            } => {
                let need_reopen = active_output
                    .as_ref()
                    .map(|o| o.sample_rate != sample_rate || o.channels != channels)
                    .unwrap_or(true);

                let result = if need_reopen {
                    match open_output_stream(shared.clone(), sample_rate, channels) {
                        Ok(output) => {
                            active_output = Some(output);
                            Ok(())
                        }
                        Err(e) => {
                            active_output = None;
                            Err(e.to_string())
                        }
                    }
                } else {
                    Ok(())
                };

                let _ = ack_tx.send(result);
            }
            OutputThreadCommand::Samples(chunk) => {
                if let Some(output) = active_output.as_ref() {
                    if output.sample_tx.send(chunk).is_err() {
                        active_output = None;
                    }
                } else {
                    atomic_saturating_sub_u64(&shared.queued_frames, chunk.frame_count);
                }
            }
        }
    }
}

impl AudioPlayer {
    pub fn new() -> Self {
        let (output_tx, output_rx) = std::sync::mpsc::sync_channel::<OutputThreadCommand>(16);
        let shared = Arc::new(SharedState {
            playing: AtomicBool::new(false),
            paused: AtomicBool::new(false),
            stop_signal: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            playback_base_position_ms: AtomicU64::new(0),
            played_frames: AtomicU64::new(0),
            queued_frames: AtomicU64::new(0),
            position_ms: AtomicU64::new(0),
            duration_ms: AtomicU64::new(0),
            volume: AtomicU8::new(100),
            track_id: AtomicI32::new(0),
            queue_item_id: AtomicI32::new(0),
            output_tx,
        });

        let output_shared = shared.clone();
        std::thread::spawn(move || {
            output_thread_loop(output_rx, output_shared);
        });

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<PlayerCommand>();
        let shared_clone = shared.clone();

        std::thread::spawn(move || {
            player_thread(cmd_rx, shared_clone);
        });

        Self { cmd_tx, shared }
    }

    pub fn send(&self, cmd: PlayerCommand) {
        let _ = self.cmd_tx.send(cmd);
    }

    pub fn status(&self) -> PlayerStatus {
        let playing = self.shared.playing.load(Ordering::Relaxed);
        let paused = self.shared.paused.load(Ordering::Relaxed);
        let state = if playing && !paused {
            PlayerState::Playing
        } else if playing && paused {
            PlayerState::Paused
        } else {
            PlayerState::Stopped
        };

        PlayerStatus {
            state,
            position_ms: self.shared.position_ms.load(Ordering::Relaxed),
            duration_ms: self.shared.duration_ms.load(Ordering::Relaxed),
            track_id: self.shared.track_id.load(Ordering::Relaxed),
            queue_item_id: self.shared.queue_item_id.load(Ordering::Relaxed),
            volume: self.shared.volume.load(Ordering::Relaxed),
        }
    }
}

fn player_thread(mut cmd_rx: mpsc::UnboundedReceiver<PlayerCommand>, shared: Arc<SharedState>) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to build tokio runtime for player");

    let mut last_request: Option<PlaybackRequest> = None;

    loop {
        let cmd = match rt.block_on(cmd_rx.recv()) {
            Some(c) => c,
            None => break,
        };

        match cmd {
            PlayerCommand::Play {
                stream,
                track_id,
                queue_item_id,
                duration_ms,
                start_position_ms,
            } => {
                let req = PlaybackRequest {
                    stream,
                    track_id,
                    queue_item_id,
                    duration_ms,
                    start_position_ms,
                };
                start_playback(&shared, &req);
                last_request = Some(req);
            }
            PlayerCommand::Pause => {
                shared.paused.store(true, Ordering::SeqCst);
            }
            PlayerCommand::Resume => {
                shared.paused.store(false, Ordering::SeqCst);
            }
            PlayerCommand::Stop => {
                shared.stop_signal.store(true, Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(100));
                shared.stop_signal.store(false, Ordering::SeqCst);
                shared.playing.store(false, Ordering::SeqCst);
                shared.paused.store(false, Ordering::SeqCst);
                shared.playback_base_position_ms.store(0, Ordering::SeqCst);
                shared.played_frames.store(0, Ordering::SeqCst);
                shared.queued_frames.store(0, Ordering::SeqCst);
                shared.position_ms.store(0, Ordering::SeqCst);
                shared.track_id.store(0, Ordering::SeqCst);
                shared.queue_item_id.store(0, Ordering::SeqCst);
            }
            PlayerCommand::Seek(position_ms) => {
                if let Some(mut req) = last_request.clone() {
                    info!(
                        "Player seek command: target={}ms track_id={} qi={}",
                        position_ms, req.track_id, req.queue_item_id
                    );
                    let was_paused = shared.paused.load(Ordering::SeqCst);
                    req.start_position_ms = position_ms;
                    start_playback(&shared, &req);
                    if was_paused {
                        shared.paused.store(true, Ordering::SeqCst);
                    }
                    last_request = Some(req);
                } else {
                    warn!("Seek requested with no active playback request");
                }
            }
            PlayerCommand::SetVolume(vol) => {
                shared.volume.store(vol, Ordering::SeqCst);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP streaming source (streams from network, buffers first 512KB for seeks)
// ---------------------------------------------------------------------------

const HEAD_SIZE: usize = 512 * 1024;

struct HttpStreamSource {
    client: reqwest::blocking::Client,
    url: String,
    reader: reqwest::blocking::Response,
    head: Vec<u8>,
    reader_pos: u64,
    pos: u64,
    content_length: Option<u64>,
}

impl HttpStreamSource {
    fn new(
        client: reqwest::blocking::Client,
        url: String,
        response: reqwest::blocking::Response,
        content_length: Option<u64>,
    ) -> Self {
        Self {
            client,
            url,
            reader: response,
            head: Vec::new(),
            reader_pos: 0,
            pos: 0,
            content_length,
        }
    }

    fn reopen_at(&mut self, start: u64) -> io::Result<()> {
        let range = format!("bytes={}-", start);
        let mut response = self
            .client
            .get(&self.url)
            .header(reqwest::header::RANGE, range)
            .send()
            .map_err(|e| io::Error::other(format!("failed range request: {}", e)))?;

        let status = response.status();
        if !status.is_success() {
            return Err(io::Error::other(format!(
                "range request failed with status {}",
                status
            )));
        }

        if start > 0 && status == reqwest::StatusCode::OK {
            // Server ignored Range; discard bytes manually to reach target.
            let mut remaining = start;
            let mut discard = [0u8; 8192];
            while remaining > 0 {
                let want = (remaining as usize).min(discard.len());
                let n = response.read(&mut discard[..want])?;
                if n == 0 {
                    break;
                }
                remaining -= n as u64;
            }
            if remaining > 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "could not skip to requested byte offset",
                ));
            }
        }

        self.reader = response;
        self.reader_pos = start;
        self.pos = start;
        Ok(())
    }
}

impl Read for HttpStreamSource {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let pos = self.pos as usize;

        if pos < self.head.len() {
            let avail = self.head.len() - pos;
            let n = buf.len().min(avail);
            buf[..n].copy_from_slice(&self.head[pos..pos + n]);
            self.pos += n as u64;
            return Ok(n);
        }

        if self.pos != self.reader_pos {
            self.reopen_at(self.pos)?;
        }

        let n = self.reader.read(buf)?;
        if n > 0 {
            if self.reader_pos < HEAD_SIZE as u64 {
                let capacity: usize = HEAD_SIZE.saturating_sub(self.head.len());
                let to_buf = n.min(capacity);
                if to_buf > 0 {
                    self.head.extend_from_slice(&buf[..to_buf]);
                }
            }
            self.reader_pos += n as u64;
            self.pos += n as u64;
        }
        Ok(n)
    }
}

impl Seek for HttpStreamSource {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let cl = self.content_length.unwrap_or(u64::MAX);
        let target: u64 = match from {
            SeekFrom::Start(n) => n,
            SeekFrom::End(n) if n < 0 => cl.saturating_sub((-n) as u64),
            SeekFrom::End(_) => cl,
            SeekFrom::Current(n) if n >= 0 => self.pos.saturating_add(n as u64),
            SeekFrom::Current(n) => self.pos.saturating_sub((-n) as u64),
        };

        if target == self.pos {
            return Ok(self.pos);
        }

        if target < self.reader_pos {
            if target < self.head.len() as u64 {
                self.pos = target;
                return Ok(self.pos);
            }
            self.reopen_at(target)?;
            return Ok(self.pos);
        }

        // Forward seek: read and discard
        let mut remaining = target - self.reader_pos;
        while remaining > 0 {
            let mut discard = [0u8; 8192];
            let want = (remaining as usize).min(discard.len());
            match self.reader.read(&mut discard[..want]) {
                Ok(0) => break,
                Ok(n) => {
                    if self.reader_pos < HEAD_SIZE as u64 {
                        let capacity: usize = HEAD_SIZE.saturating_sub(self.head.len());
                        let to_buf = n.min(capacity);
                        if to_buf > 0 {
                            self.head.extend_from_slice(&discard[..to_buf]);
                        }
                    }
                    self.reader_pos += n as u64;
                    remaining -= n as u64;
                }
                Err(e) => return Err(e),
            }
        }
        self.pos = self.reader_pos;
        Ok(self.pos)
    }
}

impl MediaSource for HttpStreamSource {
    fn is_seekable(&self) -> bool {
        true
    }

    fn byte_len(&self) -> Option<u64> {
        self.content_length
    }
}

// ---------------------------------------------------------------------------
// Streaming playback
// ---------------------------------------------------------------------------

const QBZ1_UUID: [u8; 16] = [
    0x3b, 0x42, 0x12, 0x92, 0x56, 0xf3, 0x5f, 0x75, 0x92, 0x36, 0x63, 0xb6, 0x9a, 0x1f, 0x52, 0xb2,
];

fn read_u32_be(data: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap())
}

fn read_u24_be(data: &[u8], offset: usize) -> u32 {
    ((data[offset] as u32) << 16) | ((data[offset + 1] as u32) << 8) | (data[offset + 2] as u32)
}

fn find_dfla_blocks(data: &[u8]) -> Option<Vec<u8>> {
    let mut pos = 0usize;
    while pos + 8 <= data.len() {
        let size = match data[pos..pos + 4].try_into().ok().map(u32::from_be_bytes) {
            Some(s) if s >= 8 && pos + s as usize <= data.len() => s as usize,
            _ => break,
        };
        let box_type = &data[pos + 4..pos + 8];

        if box_type == b"dfLa" {
            let body = &data[pos + 8..pos + size];
            if body.len() > 4 {
                return Some(body[4..].to_vec());
            }
        }

        let inner = match box_type {
            b"moov" | b"trak" | b"mdia" | b"minf" | b"stbl" => Some(&data[pos + 8..pos + size]),
            b"stsd" => data.get(pos + 16..pos + size),
            b"fLaC" => data.get(pos + 36..pos + size),
            _ => None,
        };
        if let Some(inner_data) = inner {
            if let Some(found) = find_dfla_blocks(inner_data) {
                return Some(found);
            }
        }
        pos += size;
    }
    None
}

fn extract_flac_header(init_data: &[u8]) -> Option<Vec<u8>> {
    let blocks = find_dfla_blocks(init_data)?;
    let mut out = Vec::with_capacity(4 + blocks.len());
    out.extend_from_slice(b"fLaC");
    out.extend_from_slice(&blocks);
    Some(out)
}

fn parse_track_key(encryption_key_hex: Option<&str>) -> Option<[u8; 16]> {
    let hex_str = encryption_key_hex?;
    let bytes = hex::decode(hex_str).ok()?;
    if bytes.len() != 16 {
        return None;
    }
    bytes.try_into().ok()
}

fn decrypt_and_extract_frames(data: &mut [u8], key: Option<&[u8; 16]>) -> Vec<u8> {
    let mut frames = Vec::new();
    let mut pos = 0usize;

    while pos + 8 <= data.len() {
        let box_size = read_u32_be(data, pos) as usize;
        if box_size < 8 || pos + box_size > data.len() {
            break;
        }

        if data[pos + 4..pos + 8] == *b"uuid"
            && box_size >= 36
            && data[pos + 8..pos + 24] == QBZ1_UUID
        {
            let body = pos + 24;
            if body + 12 > data.len() {
                pos += box_size;
                continue;
            }

            let raw_offset = read_u32_be(data, body + 4) as usize;
            let num_samples = read_u24_be(data, body + 9) as usize;
            let sample_data_start = pos + raw_offset;
            let table_start = body + 12;

            let mut offset = sample_data_start;
            for i in 0..num_samples {
                let entry = table_start + i * 16;
                if entry + 16 > data.len() {
                    break;
                }
                let size = read_u32_be(data, entry) as usize;
                let encrypted = u16::from_be_bytes([data[entry + 6], data[entry + 7]]) != 0;
                let end = offset + size;
                if end <= data.len() {
                    if encrypted {
                        if let Some(track_key) = key {
                            let mut iv = [0u8; 16];
                            iv[..8].copy_from_slice(&data[entry + 8..entry + 16]);
                            Ctr128BE::<Aes128>::new(track_key.into(), (&iv).into())
                                .apply_keystream(&mut data[offset..end]);
                        }
                    }
                    frames.extend_from_slice(&data[offset..end]);
                }
                offset += size;
            }
        }

        pos += box_size;
    }

    frames
}

struct SegmentedStreamSource {
    client: reqwest::blocking::Client,
    url_template: String,
    n_segments: usize,
    next_segment: usize,
    include_segment_one: bool,
    key: Option<[u8; 16]>,
    pending: VecDeque<u8>,
    finished: bool,
    prefetch_segment: Option<usize>,
    prefetch_result: Option<Arc<std::sync::Mutex<Option<io::Result<Vec<u8>>>>>>,
    prefetch_ready: Option<Arc<AtomicBool>>,
}

fn fetch_segment_with_client(
    client: &reqwest::blocking::Client,
    url_template: &str,
    segment: usize,
) -> io::Result<Vec<u8>> {
    let url = url_template.replace("$SEGMENT$", &segment.to_string());
    for attempt in 0..3 {
        if attempt > 0 {
            std::thread::sleep(std::time::Duration::from_millis(300 * attempt as u64));
        }
        match client.get(&url).send() {
            Ok(mut response) if response.status().is_success() => {
                let mut data = Vec::with_capacity(response.content_length().unwrap_or(0) as usize);
                response
                    .copy_to(&mut data)
                    .map_err(|e| io::Error::other(format!("segment read failed: {}", e)))?;
                return Ok(data);
            }
            Ok(response) => {
                if attempt == 2 {
                    return Err(io::Error::other(format!(
                        "segment HTTP {} for {}",
                        response.status(),
                        url
                    )));
                }
            }
            Err(e) => {
                if attempt == 2 {
                    return Err(io::Error::other(format!("segment fetch failed: {}", e)));
                }
            }
        }
    }
    Err(io::Error::other("segment fetch retries exhausted"))
}

impl SegmentedStreamSource {
    fn fetch_segment(&self, segment: usize) -> io::Result<Vec<u8>> {
        fetch_segment_with_client(&self.client, &self.url_template, segment)
    }

    fn next_segment_candidate(&self) -> Option<usize> {
        if self.include_segment_one {
            Some(1)
        } else if self.next_segment <= self.n_segments {
            Some(self.next_segment)
        } else {
            None
        }
    }

    fn ensure_prefetch(&mut self) {
        if self.prefetch_result.is_some() {
            return;
        }
        let Some(segment) = self.next_segment_candidate() else {
            return;
        };

        let client = self.client.clone();
        let url_template = self.url_template.clone();
        let result_slot = Arc::new(std::sync::Mutex::new(None));
        let ready = Arc::new(AtomicBool::new(false));
        let result_slot_bg = result_slot.clone();
        let ready_bg = ready.clone();
        std::thread::spawn(move || {
            let result = fetch_segment_with_client(&client, &url_template, segment);
            if let Ok(mut slot) = result_slot_bg.lock() {
                *slot = Some(result);
            }
            ready_bg.store(true, Ordering::Release);
        });

        self.prefetch_segment = Some(segment);
        self.prefetch_result = Some(result_slot);
        self.prefetch_ready = Some(ready);
    }

    fn fill_pending(&mut self) -> io::Result<()> {
        if self.finished || !self.pending.is_empty() {
            return Ok(());
        }

        let segment_to_fetch = if self.include_segment_one {
            self.include_segment_one = false;
            1usize
        } else {
            if self.next_segment > self.n_segments {
                self.finished = true;
                return Ok(());
            }
            let seg = self.next_segment;
            self.next_segment += 1;
            seg
        };

        let mut data = if self.prefetch_segment == Some(segment_to_fetch) {
            let result_slot = self.prefetch_result.take();
            let ready = self.prefetch_ready.take();
            self.prefetch_segment = None;
            match (result_slot, ready) {
                (Some(slot), Some(ready_flag)) => {
                    while !ready_flag.load(Ordering::Acquire) {
                        std::thread::sleep(std::time::Duration::from_millis(2));
                    }
                    let mut guard = slot
                        .lock()
                        .map_err(|_| io::Error::other("segment prefetch lock poisoned"))?;
                    guard
                        .take()
                        .ok_or_else(|| io::Error::other("segment prefetch missing result"))??
                }
                (None, _) => self.fetch_segment(segment_to_fetch)?,
                _ => self.fetch_segment(segment_to_fetch)?,
            }
        } else {
            self.fetch_segment(segment_to_fetch)?
        };
        let frames = decrypt_and_extract_frames(&mut data, self.key.as_ref());
        if frames.is_empty() {
            self.pending.extend(data);
        } else {
            self.pending.extend(frames);
        }

        self.ensure_prefetch();
        Ok(())
    }
}

impl Read for SegmentedStreamSource {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.fill_pending()?;
        if self.pending.is_empty() {
            return Ok(0);
        }

        let n = out.len().min(self.pending.len());
        for slot in out.iter_mut().take(n) {
            *slot = self.pending.pop_front().unwrap_or(0);
        }
        Ok(n)
    }
}

impl Seek for SegmentedStreamSource {
    fn seek(&mut self, _from: SeekFrom) -> io::Result<u64> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "segmented source is not seekable",
        ))
    }
}

impl MediaSource for SegmentedStreamSource {
    fn is_seekable(&self) -> bool {
        false
    }

    fn byte_len(&self) -> Option<u64> {
        None
    }
}

fn play_stream(
    stream: &StreamSource,
    shared: Arc<SharedState>,
    generation: u64,
    start_position_ms: u64,
) -> anyhow::Result<()> {
    match stream {
        StreamSource::DirectUrl(url) => {
            play_direct_stream(url, shared, generation, start_position_ms)
        }
        StreamSource::Segmented {
            url_template,
            n_segments,
            encryption_key_hex,
        } => play_segmented_stream(
            url_template,
            *n_segments,
            encryption_key_hex.as_deref(),
            shared,
            generation,
            start_position_ms,
        ),
    }
}

fn play_direct_stream(
    url: &str,
    shared: Arc<SharedState>,
    generation: u64,
    start_position_ms: u64,
) -> anyhow::Result<()> {
    info!("Streaming audio...");
    let client = reqwest::blocking::Client::new();
    let response = client.get(url).send()?;
    let content_length = response.content_length();
    let source = HttpStreamSource::new(client, url.to_string(), response, content_length);
    let mss = MediaSourceStream::new(Box::new(source), Default::default());

    let hint = Hint::new();
    let probed = symphonia::default::get_probe().format(
        &hint,
        mss,
        &FormatOptions::default(),
        &MetadataOptions::default(),
    )?;

    let mut format = probed.format;
    let track = format
        .tracks()
        .iter()
        .find(|t| t.codec_params.codec != symphonia::core::codecs::CODEC_TYPE_NULL)
        .ok_or_else(|| anyhow::anyhow!("no audio track"))?
        .clone();

    let track_id = track.id;
    let sample_rate = track.codec_params.sample_rate.unwrap_or(44100);
    let channels = track.codec_params.channels.map(|c| c.count()).unwrap_or(2);

    // Update duration from codec if available (and not already set from API)
    if shared.duration_ms.load(Ordering::Relaxed) == 0 {
        if let Some(n_frames) = track.codec_params.n_frames {
            let dur_ms = (n_frames as f64 / sample_rate as f64 * 1000.0) as u64;
            shared.duration_ms.store(dur_ms, Ordering::SeqCst);
        }
    }

    let mut decoder =
        symphonia::default::get_codecs().make(&track.codec_params, &DecoderOptions::default())?;

    let mut base_position_ms = 0u64;
    if start_position_ms > 0 {
        let seek_time = Time::from(start_position_ms as f64 / 1000.0);
        match format.seek(
            SeekMode::Accurate,
            SeekTo::Time {
                time: seek_time,
                track_id: Some(track_id),
            },
        ) {
            Ok(_) => {
                decoder.reset();
                base_position_ms = start_position_ms;
                shared.position_ms.store(base_position_ms, Ordering::SeqCst);
                info!("Seeked playback to {}ms", start_position_ms);
            }
            Err(e) => {
                warn!("Seek to {}ms failed: {}", start_position_ms, e);
            }
        }
    }

    shared
        .playback_base_position_ms
        .store(base_position_ms, Ordering::SeqCst);
    shared.played_frames.store(0, Ordering::SeqCst);
    shared.position_ms.store(base_position_ms, Ordering::SeqCst);

    ensure_audio_output(&shared, sample_rate, channels)?;
    info!("Playback started ({}Hz, {}ch)", sample_rate, channels);

    let mut finished_naturally = false;

    loop {
        // Check if superseded by a newer Play command (generation changed)
        if shared.generation.load(Ordering::SeqCst) != generation {
            info!("Playback superseded by newer generation");
            break;
        }

        if shared.stop_signal.load(Ordering::Relaxed) {
            info!("Playback stopped by signal");
            break;
        }

        while shared.paused.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_millis(50));
            if shared.stop_signal.load(Ordering::Relaxed)
                || shared.generation.load(Ordering::SeqCst) != generation
            {
                break;
            }
        }
        if shared.stop_signal.load(Ordering::Relaxed)
            || shared.generation.load(Ordering::SeqCst) != generation
        {
            break;
        }

        let packet = match format.next_packet() {
            Ok(p) => p,
            Err(symphonia::core::errors::Error::IoError(ref e))
                if e.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                info!("Playback finished (gen={})", generation);
                finished_naturally = true;
                break;
            }
            Err(symphonia::core::errors::Error::ResetRequired) => {
                decoder.reset();
                continue;
            }
            Err(e) => {
                warn!("Packet error: {}", e);
                break;
            }
        };

        if shared.generation.load(Ordering::SeqCst) != generation {
            info!("Playback packet discarded after generation switch");
            break;
        }
        if shared.stop_signal.load(Ordering::Relaxed) {
            info!("Playback packet discarded due to stop signal");
            break;
        }

        if packet.track_id() != track_id {
            continue;
        }

        let decoded = match decoder.decode(&packet) {
            Ok(d) => d,
            Err(symphonia::core::errors::Error::DecodeError(e)) => {
                warn!("Decode error: {}", e);
                continue;
            }
            Err(e) => {
                warn!("Decode error: {}", e);
                break;
            }
        };

        let spec = *decoded.spec();
        let n_frames = decoded.frames();
        let mut sample_buf = SampleBuffer::<f32>::new(n_frames as u64, spec);
        sample_buf.copy_interleaved_ref(decoded);
        let samples = sample_buf.samples();
        let frame_count = (samples.len() / channels) as u64;
        if frame_count == 0 {
            continue;
        }
        let samples_vec = samples.to_vec();

        if !queue_samples_for_playback(&shared, generation, channels, samples_vec) {
            break;
        }
    }

    if finished_naturally {
        let drain_deadline = std::time::Instant::now() + std::time::Duration::from_secs(12);
        while shared.queued_frames.load(Ordering::Relaxed) > 0 {
            if shared.generation.load(Ordering::SeqCst) != generation {
                break;
            }
            if shared.stop_signal.load(Ordering::Relaxed) {
                break;
            }
            if std::time::Instant::now() >= drain_deadline {
                warn!(
                    "Playback drain timeout with {} queued frames",
                    shared.queued_frames.load(Ordering::Relaxed)
                );
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    } else {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Only clear playing state if we're still the current generation
    // (if generation changed, a new Play command has taken over — don't clobber its state)
    if shared.generation.load(Ordering::SeqCst) == generation {
        shared.playing.store(false, Ordering::SeqCst);
        shared.paused.store(false, Ordering::SeqCst);
    }
    Ok(())
}

fn play_segmented_stream(
    url_template: &str,
    n_segments: u32,
    encryption_key_hex: Option<&str>,
    shared: Arc<SharedState>,
    generation: u64,
    start_position_ms: u64,
) -> anyhow::Result<()> {
    if n_segments == 0 {
        return Err(anyhow::anyhow!("segmented stream has zero segments"));
    }

    info!(
        "Streaming segmented audio (segments={}, start={}ms)",
        n_segments, start_position_ms
    );

    let client = reqwest::blocking::Client::new();
    let segment_duration_ms = 10_000u64;
    let mut start_segment = ((start_position_ms / segment_duration_ms) + 1) as usize;
    start_segment = start_segment.clamp(1, n_segments as usize);
    let segment_start_ms = (start_segment as u64 - 1) * segment_duration_ms;
    let mut skip_within_segment_ms = start_position_ms.saturating_sub(segment_start_ms);

    let mut pending = VecDeque::new();
    let init_url = url_template.replace("$SEGMENT$", "0");
    let mut is_flac = false;
    if let Ok(response) = client.get(&init_url).send() {
        if response.status().is_success() {
            if let Ok(init_bytes) = response.bytes() {
                if let Some(header) = extract_flac_header(&init_bytes) {
                    pending.extend(header);
                    is_flac = true;
                }
            }
        }
    }

    let key = parse_track_key(encryption_key_hex);
    let include_segment_one = key.is_none() && start_segment > 1;
    if include_segment_one {
        skip_within_segment_ms = skip_within_segment_ms.saturating_add(segment_duration_ms);
    }

    let mut source = SegmentedStreamSource {
        client,
        url_template: url_template.to_string(),
        n_segments: n_segments as usize,
        next_segment: start_segment,
        include_segment_one,
        key,
        pending,
        finished: false,
        prefetch_segment: None,
        prefetch_result: None,
        prefetch_ready: None,
    };
    source.ensure_prefetch();
    let mss = MediaSourceStream::new(Box::new(source), Default::default());

    let mut hint = Hint::new();
    if is_flac {
        hint.with_extension("flac");
    }
    let probed = symphonia::default::get_probe().format(
        &hint,
        mss,
        &FormatOptions::default(),
        &MetadataOptions::default(),
    )?;

    let mut format = probed.format;
    let track = format
        .tracks()
        .iter()
        .find(|t| t.codec_params.codec != symphonia::core::codecs::CODEC_TYPE_NULL)
        .ok_or_else(|| anyhow::anyhow!("no audio track"))?
        .clone();

    let track_id = track.id;
    let sample_rate = track.codec_params.sample_rate.unwrap_or(44100);
    let channels = track.codec_params.channels.map(|c| c.count()).unwrap_or(2);
    let mut decoder =
        symphonia::default::get_codecs().make(&track.codec_params, &DecoderOptions::default())?;

    shared
        .playback_base_position_ms
        .store(start_position_ms, Ordering::SeqCst);
    shared.played_frames.store(0, Ordering::SeqCst);
    shared
        .position_ms
        .store(start_position_ms, Ordering::SeqCst);

    ensure_audio_output(&shared, sample_rate, channels)?;
    info!("Playback started ({}Hz, {}ch)", sample_rate, channels);

    let mut finished_naturally = false;
    let mut samples_to_skip = ((skip_within_segment_ms as u128)
        .saturating_mul(sample_rate as u128)
        .saturating_mul(channels as u128)
        / 1000) as u64;

    loop {
        if shared.generation.load(Ordering::SeqCst) != generation {
            info!("Playback superseded by newer generation");
            break;
        }
        if shared.stop_signal.load(Ordering::Relaxed) {
            info!("Playback stopped by signal");
            break;
        }

        while shared.paused.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_millis(50));
            if shared.stop_signal.load(Ordering::Relaxed)
                || shared.generation.load(Ordering::SeqCst) != generation
            {
                break;
            }
        }
        if shared.stop_signal.load(Ordering::Relaxed)
            || shared.generation.load(Ordering::SeqCst) != generation
        {
            break;
        }

        let packet = match format.next_packet() {
            Ok(p) => p,
            Err(symphonia::core::errors::Error::IoError(ref e))
                if e.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                info!("Playback finished (gen={})", generation);
                finished_naturally = true;
                break;
            }
            Err(symphonia::core::errors::Error::ResetRequired) => {
                decoder.reset();
                continue;
            }
            Err(e) => {
                warn!("Packet error: {}", e);
                break;
            }
        };

        if packet.track_id() != track_id {
            continue;
        }

        let decoded = match decoder.decode(&packet) {
            Ok(d) => d,
            Err(symphonia::core::errors::Error::DecodeError(e)) => {
                warn!("Decode error: {}", e);
                continue;
            }
            Err(e) => {
                warn!("Decode error: {}", e);
                break;
            }
        };

        let spec = *decoded.spec();
        let n_frames = decoded.frames();
        let mut sample_buf = SampleBuffer::<f32>::new(n_frames as u64, spec);
        sample_buf.copy_interleaved_ref(decoded);
        let samples = sample_buf.samples();
        if samples.is_empty() {
            continue;
        }

        let start_offset = if samples_to_skip == 0 {
            0usize
        } else if samples_to_skip >= samples.len() as u64 {
            samples_to_skip -= samples.len() as u64;
            continue;
        } else {
            let off = samples_to_skip as usize;
            samples_to_skip = 0;
            off
        };

        let samples_vec = samples[start_offset..].to_vec();
        let frame_count = (samples_vec.len() / channels) as u64;
        if frame_count == 0 {
            continue;
        }

        if !queue_samples_for_playback(&shared, generation, channels, samples_vec) {
            break;
        }
    }

    if finished_naturally {
        let drain_deadline = std::time::Instant::now() + std::time::Duration::from_secs(12);
        while shared.queued_frames.load(Ordering::Relaxed) > 0 {
            if shared.generation.load(Ordering::SeqCst) != generation {
                break;
            }
            if shared.stop_signal.load(Ordering::Relaxed) {
                break;
            }
            if std::time::Instant::now() >= drain_deadline {
                warn!(
                    "Playback drain timeout with {} queued frames",
                    shared.queued_frames.load(Ordering::Relaxed)
                );
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    } else {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    if shared.generation.load(Ordering::SeqCst) == generation {
        shared.playing.store(false, Ordering::SeqCst);
        shared.paused.store(false, Ordering::SeqCst);
    }
    Ok(())
}
