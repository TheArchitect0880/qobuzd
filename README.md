# qobuzd

A headless Qobuz Connect (QConnect) renderer for Linux, written in Rust.

This was made by reverse engineering the Qobuz Android mobile app to figure out the API, authentication flow, qbz-1 audio stream encryption, and the QConnect WebSocket protocol. I didn't know Rust and needed something working very quickly, so I used AI to write most of the code for me. As a result, the codebase is not something I would recommend using. It works, but it is AI slop.

We only found out about [qbz](https://github.com/vicrodh/qbz) when publishing this to GitHub. If you are looking for a proper Qobuz desktop client or QConnect renderer, use that instead. It is a much more polished and better implemented project.

Since this is a headless CLI tool with no GUI, resource usage should be lower than a full desktop client like [qobuz-qt](https://github.com/TheArchitect0880/qobuz-qt).

## Features

- Qobuz Connect renderer: appears as a selectable device in the official Qobuz mobile/desktop app
- Hand-rolled protobuf encoding/decoding for the QConnect WebSocket protocol (no protobuf dependency)
- Login via Qobuz email/password (OAuth2)
- Token storage and refresh via system keyring
- Device linking support
- Audio playback with support for Hi-Res 24-bit/192 kHz, Hi-Res 24-bit/96 kHz, CD quality 16-bit lossless, and MP3 320 kbps
- Segmented streaming with qbz-1 AES-128-CTR decryption
- Direct URL streaming with HTTP Range seek support
- FLAC header extraction from MP4 containers for segmented streams
- Seek, pause, resume, volume control
- Automatic playback state reporting to the Qobuz app (position, buffering, track ended)

## Commands

```
qobuzd login --email <email> --password <password>
qobuzd logout
qobuzd status
qobuzd user
qobuzd serve
```

The `serve` command starts the QConnect renderer. Select it in the Qobuz app to play music.

## Building

```
cargo build --release
```

The binary will be at `target/release/qobuzd`.

## License

MIT license.
