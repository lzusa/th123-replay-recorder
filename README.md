# TH123 Replay Recorder

A Python service that automatically records replays from spectatable Touhou 12.3 (Hisoutensoku) games.

## Overview

This tool connects to the Touhou 12.3 online server, discovers spectatable games, and automatically records match replays to `.rep` files. It supports parallel recording of multiple games simultaneously.

## Protocol Reference

This implementation is based on:
- [Touhou 12.3 Protocol Documentation](https://github.com/delthas/touhou-protocol-docs) by delthas
- [非想天则REP文件结构解析](https://rinkastone.com/2022/01/10/archives/109) by Rinkastone (石塔)

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

The `.rep` file format follows the official Hisoutensoku 1.10 client format. This section is based on reverse engineering work by the community.

### Header Structure (0x00 - 0x78)

```
Offset  Size    Description
------  ----    -----------
0x00    2       Version marker (0xD2 0x00 for 1.10)
0x02    4       Unknown/Reserved (0x0004 may be a random seed related to Reisen/Yuyuko bullets)
0x06    2       Unknown (0x03 0x09)
0x08    1       Mode marker (0x06 for PvP network)
0x09    5       Unknown/Reserved (observed as 00 00 01 03 00)
0x0E    50      Host player data block (1P)
0x40    50      Client player data block (2P)
0x72    1       Unknown (observed as 0x03)
0x73    1       Stage ID
0x74    1       Music ID
0x75    4       Random seed (uint32 LE)
0x79    4       Game input count (uint32 LE) - determines replay duration
```

### Player Data Block Structure (50 bytes)

```
Offset  Size    Description
------  ----    -----------
0x00    1       Character ID (0x00-0x13, see table below)
0x01    1       Skin/Palette ID (0x00-0x07 normal, 0x08+ special colors)
0x02    4       Deck card count (uint32 LE, usually 0x14 = 20 cards)
0x06    40      Deck cards (20 x uint16 LE, card IDs)
0x2E    1       Side marker (0x00=host/1P, 0x01=client/2P)
0x2F    1       Unknown (observed as 0x01)
0x30    1       Disabled simultaneous buttons flag (0x00 or 0x01)
```

**Note on offsets:** The player data blocks are at:
- Host (1P): 0x0E - 0x3D
- Client (2P): 0x3E - 0x6D

### Input Data (after header, offset 0x7D+)

Each frame consists of two 16-bit input words (little-endian):
- First word: Host (1P) player input
- Second word: Client (2P) player input

**Input bit flags during battle:**
| Bit | Binary | Hex | Button |
|-----|--------|-----|--------|
| 0 | 0b00000000 0b00000001 | 0x0001 | A+B |
| 1 | 0b00000000 0b00000010 | 0x0002 | B+C |
| 8 | 0b00000001 0b00000000 | 0x0100 | Up |
| 9 | 0b00000010 0b00000000 | 0x0200 | Down |
| 10 | 0b00000100 0b00000000 | 0x0400 | Left |
| 11 | 0b00001000 0b00000000 | 0x0800 | Right |
| 12 | 0b00010000 0b00000000 | 0x1000 | A |
| 13 | 0b00100000 0b00000000 | 0x2000 | B |
| 14 | 0b01000000 0b00000000 | 0x4000 | C |
| 15 | 0b10000000 0b00000000 | 0x8000 | Dash |

### Character IDs

| ID | Character | Special Colors |
|----|-----------|----------------|
| 0x00 | Reimu | 1 |
| 0x01 | Marisa | 1 |
| 0x02 | Sakuya | 1 |
| 0x03 | Alice | 1 |
| 0x04 | Patchouli | 1 |
| 0x05 | Youmu | 1 |
| 0x06 | Remilia | 2 |
| 0x07 | Yuyuko | 1 |
| 0x08 | Yukari | 1 (null/black) |
| 0x09 | Suika | 1 |
| 0x0A | Reisen | 1 (null/black) |
| 0x0B | Aya | 1 |
| 0x0C | Komachi | 1 |
| 0x0D | Iku | 2 |
| 0x0E | Tenshi | 2 |
| 0x0F | Sanae | 1 |
| 0x10 | Cirno | 2 (null/black) |
| 0x11 | Meiling | 3 |
| 0x12 | Utsuho | 1 |
| 0x13 | Suwako | 1 |

### Stage IDs

| ID | Stage |
|----|-------|
| 0x00 | Collapsed Hakurei Shrine |
| 0x01 | Forest of Magic |
| 0x02 | Genbu Ravine |
| 0x03 | Youkai Mountain |
| 0x04 | Unidentified Fantastic Object |
| 0x05 | Bhava-agra |
| 0x0A | Hakurei Shrine |
| 0x0B | Kirisame Magic Shop |
| 0x0C | Scarlet Devil Mansion Clock Tower |
| 0x0D | Forest of Dolls |
| 0x0E | Scarlet Devil Mansion Library |
| 0x0F | Netherworld |
| 0x10 | Scarlet Devil Mansion Lobby |
| 0x11 | Hakugyokurou Snow Garden |
| 0x12 | Bamboo Forest of the Lost |
| 0x1E | Misty Lake |
| 0x1F | Moriya Shrine |
| 0x20 | Underground Geyser Center Entrance |
| 0x21 | Underground Geyser Center Passage |
| 0x22 | Fusion Reactor Core |

## Installation

```bash
# Clone or download the script
git clone git@github.com:lzusa/th123-replay-recorder.git
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
| `--only-cn` | | - | Only spectate games where both players' country is 'cn' |

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
    - When gaps are detected, the spectator will send targeted "missing frame" requests to backfill.
        - By default the code will send a small burst of repeat requests for the earliest missing pair-frame to accelerate recovery (configurable via the `miss_multiplier` parameter passed to `capture_replay()`, default 10).
        - This aggressive behavior is limited to a burst per main loop iteration to avoid indefinitely flooding the network; adjust `miss_multiplier` or disable the feature if you observe increased packet loss or latency.
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

## References

- [Touhou 12.3 Protocol Documentation](https://github.com/delthas/touhou-protocol-docs) - Official protocol docs by delthas
- [非想天则REP文件结构解析](https://rinkastone.com/2022/01/10/archives/109) - Detailed .rep file format analysis by Rinkastone
- [Deck Extracting Wiki](https://hisouten.koumakan.jp/wiki/Deck_Extracting) - Card, character, stage, and BGM value tables

## License

This project is provided as-is for educational and personal use. Please respect the Touhou 12.3 community guidelines when using this tool.

## Acknowledgments

- Protocol documentation by [delthas](https://github.com/delthas/touhou-protocol-docs)
- REP file format analysis by [Rinkastone](https://rinkastone.com/)
- Touhou 12.3 (Hisoutensoku) by Twilight Frontier and Team Shanghai Alice
