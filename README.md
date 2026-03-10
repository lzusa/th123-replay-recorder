# TH123 Replay Recorder

A Python service that automatically records replays from spectatable Touhou 12.3 (Hisoutensoku) games.

## Overview

This tool connects to the Touhou 12.3 online server, discovers spectatable games, and automatically records match replays to `.rep` files. It supports parallel recording of multiple games simultaneously.

## Protocol Reference

This implementation is based on the [Touhou 12.3 Protocol Documentation](https://github.com/delthas/touhou-protocol-docs) by delthas.

### Key Protocol Details

The Touhou 12.3 network protocol uses UDP datagrams with a tree topology:
- **Host**: The game creator
- **Client**: The player who joins the host
- **Spectators**: Observers connected to the tree (max 4 children per spectator node)

### Packet Types

| Packet | Value | Description |
|--------|-------|-------------|
| HELLO | 0x01 | Initial handshake |
| PUNCH | 0x02 | UDP hole punching |
| OLLEH | 0x03 | Connection accepted |
| CHAIN | 0x04 | Tree maintenance |
| INIT_REQUEST | 0x05 | Session initialization |
| INIT_SUCCESS | 0x06 | Session accepted |
| INIT_ERROR | 0x07 | Session rejected |
| REDIRECT | 0x08 | Redirect to child node |
| QUIT | 0x0B | Session termination |
| HOST_GAME | 0x0D | Game data from host |
| CLIENT_GAME | 0x0E | Game data from client/spectator |

### Game Packet Sub-types

| Sub-type | Value | Description |
|----------|-------|-------------|
| GAME_LOADED | 0x01 | Scene loaded notification |
| GAME_LOADED_ACK | 0x02 | Scene load acknowledgment |
| GAME_INPUT | 0x03 | Player inputs |
| GAME_MATCH | 0x04 | Match configuration |
| GAME_MATCH_ACK | 0x05 | Match config acknowledgment |
| GAME_MATCH_REQUEST | 0x08 | Request match config |
| GAME_REPLAY | 0x09 | Replay data (compressed) |
| GAME_REPLAY_REQUEST | 0x0B | Request replay data |

## Replay File Format (.rep)

The `.rep` file format follows the official Hisoutensoku 1.10 client format:

### Header Structure (0x00 - 0x76)

```
Offset  Size    Description
------  ----    -----------
0x00    2       Version marker (0xD2 0x00 for 1.10)
0x02    4       Unknown/Reserved
0x06    2       Unknown (0x03 0x09)
0x08    1       Mode marker (0x06 for PvP)
0x09    5       Unknown/Reserved
0x0E    50      Host player data block
0x40    50      Client player data block
0x72    1       Unknown (0x03)
0x73    1       Stage ID
0x74    1       Music ID
0x75    4       Random seed (uint32 LE)
0x79    4       Game input count (uint32 LE)
```

### Player Data Block Structure (50 bytes)

```
Offset  Size    Description
------  ----    -----------
0x00    1       Character ID
0x01    1       Skin/Palette ID
0x02    1       Deck ID (unused)
0x03    1       Deck size (usually 20)
0x04    40      Deck cards (20 x uint16 LE)
0x2C    1       Side marker (0x00=host, 0x01=client)
0x2D    1       Padding (0x00)
0x2E    1       Disabled simultaneous buttons flag
```

### Input Data (after header)

Each frame consists of two 16-bit input words:
- First word: Host player input
- Second word: Client player input

Input bit flags during battle:
| Button | Binary | Hex |
|--------|--------|-----|
| A+B | 0b00000000 0b00000001 | 0x0001 |
| B+C | 0b00000000 0b00000010 | 0x0002 |
| Up | 0b00000001 0b00000000 | 0x0100 |
| Down | 0b00000010 0b00000000 | 0x0200 |
| Left | 0b00000100 0b00000000 | 0x0400 |
| Right | 0b00001000 0b00000000 | 0x0800 |
| A | 0b00010000 0b00000000 | 0x1000 |
| B | 0b00100000 0b00000000 | 0x2000 |
| C | 0b01000000 0b00000000 | 0x4000 |
| Dash | 0b10000000 0b00000000 | 0x8000 |

### Character IDs

| ID | Character |
|----|-----------|
| 0x00 | Reimu |
| 0x01 | Marisa |
| 0x02 | Sakuya |
| 0x03 | Alice |
| 0x04 | Patchouli |
| 0x05 | Youmu |
| 0x06 | Remilia |
| 0x07 | Yuyuko |
| 0x08 | Yukari |
| 0x09 | Suika |
| 0x0A | Reisen |
| 0x0B | Aya |
| 0x0C | Komachi |
| 0x0D | Iku |
| 0x0E | Tenshi |
| 0x0F | Sanae |
| 0x10 | Cirno |
| 0x11 | Meiling |
| 0x12 | Utsuho |
| 0x13 | Suwako |

## Installation

```bash
# Clone or download the script
git clone <repository-url>
cd th123-replay-recorder

# No additional dependencies required (uses only Python standard library)
```

## Usage

### Basic Usage

```bash
# Run continuously, recording all spectatable games
python replay_recorder.py

# Run once and exit
python replay_recorder.py --once

# Specify output directory
python replay_recorder.py --output /path/to/replays

# Adjust poll interval (seconds between server checks)
python replay_recorder.py --poll 60

# Limit concurrent connections
python replay_recorder.py --workers 5
```

### Command Line Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--output` | `-o` | `replays/` | Output directory for replay files |
| `--poll` | `-p` | `30.0` | Poll interval in seconds |
| `--duration` | `-d` | `0` | Capture duration (0 = until match end) |
| `--workers` | `-w` | `10` | Max concurrent connections |
| `--once` | `-1` | - | Run once and exit |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `REPLAY_DEBUG` | Enable debug logging to `logs/replay_service.log` |

## Output Structure

```
replays/
├── recorded_games.txt      # List of recorded game sessions
├── replay_service.log      # Service log (if REPLAY_DEBUG set)
└── {host}-{client}_{ip}_{port}/
    ├── 260310_104522_Player1(remilia)-Player2(yuyuko).rep
    ├── 260310_104855_Player1(remilia)-Player2(yuyuko).rep
    └── ...
```

## How It Works

1. **Discovery**: Fetches the list of active games from `https://konni.delthas.fr/games`
2. **Filtering**: Selects only spectatable games that have started
3. **Connection**: Performs UDP hole punching to connect as a spectator
4. **Handshake**: Sends INIT_REQUEST and receives INIT_SUCCESS
5. **Recording**: 
   - Sends GAME_REPLAY_REQUEST every 3 frames (~50ms)
   - Receives GAME_REPLAY packets with compressed input data
   - Decompresses zlib data and extracts frame inputs
6. **Saving**: Writes captured data to `.rep` files in the official format
7. **Multi-match**: Automatically detects match switches and records multiple matches per session

## Technical Details

### Replay Data Compression

GAME_REPLAY packets contain zlib-compressed data:
```
replay_data = frame_id(4) + end_frame_id(4) + match_id(1) +
              game_inputs_count(1) + [replay_inputs_count(1)] + replay_inputs(...)
```

Each `replay_input` is 4 bytes:
- 2 bytes: Client input (uint16 LE)
- 2 bytes: Host input (uint16 LE)

### Spectator Protocol Flow

```
Spectator -> Host: HELLO
Host -> Spectator: OLLEH
Spectator -> Host: INIT_REQUEST (game_id, spectate_request=0x00)
Host -> Spectator: INIT_SUCCESS (spectate_info, player_names)
Spectator -> Host: GAME_REPLAY_REQUEST (frame_id, match_id) [every 3 frames]
Host -> Spectator: GAME_REPLAY (compressed inputs)
Host -> Spectator: GAME_MATCH (match config when new match starts)
```

## License

This project is provided as-is for educational and personal use. Please respect the Touhou 12.3 community guidelines when using this tool.

## Acknowledgments

- Protocol documentation by [delthas](https://github.com/delthas/touhou-protocol-docs)
- Touhou 12.3 (Hisoutensoku) by Twilight Frontier and Team Shanghai Alice
