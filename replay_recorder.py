#!/usr/bin/env python3
"""
Touhou 12.3 Replay Recording Service - Parallel Version
A persistent service that monitors and automatically saves replays from spectatable games.
Uses concurrent connections to capture multiple games simultaneously.
"""

import socket
import struct
import json
import zlib
import time
import binascii
import os
import sys
import argparse
import logging
import threading
import signal
import traceback
import glob
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Set
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set UTF-8 output for Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Configure logging - output to file only to keep console clean for status display
log_level = logging.DEBUG if os.environ.get("REPLAY_DEBUG") else logging.INFO

# Ensure logs directory exists
LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOGS_DIR, 'replay_service.log')
ERROR_LOG_FILE = os.path.join(LOGS_DIR, 'error_details.log')

logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
    ]
)
logger = logging.getLogger(__name__)

# Error logger for detailed error tracking
error_logger = logging.getLogger('error_details')
error_logger.setLevel(logging.ERROR)
error_handler = logging.FileHandler(ERROR_LOG_FILE, encoding='utf-8')
error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
error_logger.addHandler(error_handler)


def log_error(context: str, exception: Exception, extra_info: Dict[str, Any] = None):
    """Log detailed error information to error_details.log"""
    error_msg = f"\n{'='*60}\n"
    error_msg += f"ERROR CONTEXT: {context}\n"
    error_msg += f"{'='*60}\n"
    error_msg += f"Exception Type: {type(exception).__name__}\n"
    error_msg += f"Exception Message: {str(exception)}\n"
    error_msg += f"\nTraceback:\n{traceback.format_exc()}\n"

    if extra_info:
        error_msg += f"\nAdditional Context:\n"
        for key, value in extra_info.items():
            error_msg += f"  {key}: {value}\n"

    error_msg += f"{'='*60}\n"
    error_logger.error(error_msg)
    logger.error(f"{context}: {type(exception).__name__}: {str(exception)}")

# Packet types
PACKET_HELLO = 0x01
PACKET_PUNCH = 0x02
PACKET_OLLEH = 0x03
PACKET_CHAIN = 0x04
PACKET_INIT_REQUEST = 0x05
PACKET_INIT_SUCCESS = 0x06
PACKET_INIT_ERROR = 0x07
PACKET_REDIRECT = 0x08
PACKET_QUIT = 0x0B
PACKET_HOST_GAME = 0x0D
PACKET_CLIENT_GAME = 0x0E

# Game packet sub-types
GAME_LOADED = 0x01
GAME_LOADED_ACK = 0x02
GAME_INPUT = 0x03
GAME_MATCH = 0x04
GAME_MATCH_ACK = 0x05
GAME_MATCH_REQUEST = 0x08
GAME_REPLAY = 0x09
GAME_REPLAY_REQUEST = 0x0B  # This is game type, not packet type

# Character IDs
CHARACTERS = {
    0x00: "reimu", 0x01: "marisa", 0x02: "sakuya", 0x03: "alice",
    0x04: "patchouli", 0x05: "youmu", 0x06: "remilia", 0x07: "yuyuko",
    0x08: "yukari", 0x09: "suika", 0x0A: "reisen", 0x0B: "aya",
    0x0C: "komachi", 0x0D: "iku", 0x0E: "tenshi", 0x0F: "sanae",
    0x10: "cirno", 0x11: "meiling", 0x12: "utsuho", 0x13: "suwako"
}

# Known game IDs observed for spectating.
# Try SWR-compatible IDs first, then fall back to the no-SWR ID.
OBSERVED_SWR_GAME_ID = bytes.fromhex("6C7365D9FFC46E488D7CA19231347295")
SOKUROLL_SWR_GAME_ID = bytes.fromhex("647365D9FFC46E488D7CA19231347295")
VANILLA_SWR_GAME_ID = bytes.fromhex("6E7365D9FFC46E488D7CA19231347295")
DEFAULT_GAME_ID = bytes.fromhex("46C967C8ACF2444DB8B1ECDED4D5404A")
SPECTATE_GAME_ID_CANDIDATES = (
    OBSERVED_SWR_GAME_ID,
    SOKUROLL_SWR_GAME_ID,
    VANILLA_SWR_GAME_ID,
    DEFAULT_GAME_ID,
)
STUFF_BYTES = bytes([0x3B, 0xAA, 0x01, 0x6E, 0x28, 0x00, 0xFC, 0x30])

# Server URL
GAMES_URL = "https://konni.delthas.fr/games"
SERVER_HOST = "konni.delthas.fr"
SERVER_PORT = 11100

# Output directory
DEFAULT_OUTPUT_DIR = "replays"

# Global stats for display
@dataclass
class ActiveGameInfo:
    ip: str = ""
    host_name: str = ""
    client_name: str = ""
    host_char: str = ""
    client_char: str = ""
    match_id: int = 0
    frame_count: int = 0
    saved_count: int = 0
    status: str = ""


class ServiceStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.total = 0
        self.active = 0
        self.recorded = 0
        self.skipped = 0
        self.errors = 0
        self.current_games: Dict[str, ActiveGameInfo] = {}  # ip -> ActiveGameInfo

    def update(self, **kwargs):
        with self.lock:
            for k, v in kwargs.items():
                if hasattr(self, k):
                    setattr(self, k, v)

    def get_status_line(self) -> str:
        with self.lock:
            active_games = len(self.current_games)
            return (
                f"[已录制: {self.recorded} | 活跃: {active_games} | "
                f"已扫描: {self.total} | 跳过: {self.skipped} | 错误: {self.errors}]"
            )

    def set_game_status(self, ip: str, status: str):
        with self.lock:
            if ip in self.current_games:
                self.current_games[ip].status = status

    def set_game_info(self, ip: str, info: ActiveGameInfo):
        with self.lock:
            self.current_games[ip] = info

    def update_game_capture(self, ip: str, match_id: int, frame_count: int,
                            host_char: str = "", client_char: str = ""):
        with self.lock:
            if ip in self.current_games:
                g = self.current_games[ip]
                g.match_id = match_id
                g.frame_count = frame_count
                if host_char:
                    g.host_char = host_char
                if client_char:
                    g.client_char = client_char

    def update_game_saved_count(self, ip: str, count: int):
        with self.lock:
            if ip in self.current_games:
                self.current_games[ip].saved_count = count

    def remove_game(self, ip: str):
        with self.lock:
            if ip in self.current_games:
                del self.current_games[ip]

    def get_active_games_snapshot(self) -> List[ActiveGameInfo]:
        with self.lock:
            return list(self.current_games.values())


stats = ServiceStats()


@dataclass
class PlayerData:
    name: str = ""
    character_id: int = 0
    skin_id: int = 0
    deck_id: int = 0
    deck: List[int] = field(default_factory=list)
    disabled_simultaneous: bool = False


@dataclass
class MatchData:
    host: PlayerData = field(default_factory=PlayerData)
    client: PlayerData = field(default_factory=PlayerData)
    stage_id: int = 0
    music_id: int = 0
    random_seed: int = 0
    match_id: int = 0
    swr_disabled: int = 1


@dataclass
class FrameInput:
    frame_id: int
    client_input: int
    host_input: int


@dataclass
class ReplayData:
    match_data: Optional[MatchData] = None
    frames: List[FrameInput] = field(default_factory=list)
    ended: bool = False
    end_frame: int = 0
    debug_stop_reason: str = ""
    debug_session_signals: int = 0
    debug_match_packets: int = 0
    debug_replay_packets: int = 0
    debug_match_parse_failures: int = 0
    debug_replay_parse_failures: int = 0
    debug_last_replay_frame: int = 0
    debug_last_requested_frame: int = 0
    debug_last_match_id: int = 0
    debug_init_spectate_info: int = 0
    debug_init_host_name: str = ""
    debug_init_client_name: str = ""
    debug_init_swr_disabled: int = 0
    debug_init_game_id: str = ""


def normalize_frame_inputs(frames: List[FrameInput]) -> List[FrameInput]:
    """Normalize frames by dropping invalid IDs, deduplicating, and sorting by frame_id."""
    dedup: Dict[int, FrameInput] = {}
    for frame in frames:
        frame_id = int(frame.frame_id)
        if frame_id <= 0:
            continue
        dedup[frame_id] = FrameInput(
            frame_id=frame_id,
            client_input=int(frame.client_input) & 0xFFFF,
            host_input=int(frame.host_input) & 0xFFFF,
        )

    return [dedup[frame_id] for frame_id in sorted(dedup.keys())]


class TouhouProtocol:
    """Touhou 12.3 Protocol implementation"""

    @staticmethod
    def pack_sockaddr(addr: str, port: int) -> bytes:
        """Pack sockaddr_in structure"""
        ip_parts = list(map(int, addr.split(".")))
        # struct sockaddr_in: family (2) + port (2) + addr (4) + padding (8)
        return bytes([0x02, 0x00]) + struct.pack(">H", port) + bytes(ip_parts) + bytes(8)

    @staticmethod
    def create_hello(target_addr: str, target_port: int) -> bytes:
        """Create HELLO packet for initial handshake"""
        addr_bin = TouhouProtocol.pack_sockaddr(target_addr, target_port)
        # HELLO: type (1) + target_addr (16) + our_addr (16) + padding (4)
        return bytes([PACKET_HELLO]) + addr_bin + addr_bin + bytes([0x00, 0x00, 0x00, 0xBC])

    @staticmethod
    def create_punch(target_addr: str, target_port: int, their_addr: str, their_port: int) -> bytes:
        """Create PUNCH packet in response to HELLO"""
        target_bin = TouhouProtocol.pack_sockaddr(target_addr, target_port)
        their_bin = TouhouProtocol.pack_sockaddr(their_addr, their_port)
        return bytes([PACKET_PUNCH]) + target_bin + their_bin

    @staticmethod
    def create_olleh(target_addr: str, target_port: int) -> bytes:
        """Create OLLEH packet in response to PUNCH"""
        addr_bin = TouhouProtocol.pack_sockaddr(target_addr, target_port)
        return bytes([PACKET_OLLEH]) + addr_bin + addr_bin + bytes([0x00, 0x00, 0x00, 0xBC])

    @staticmethod
    def create_chain(target_addr: str, target_port: int, their_addr: str, their_port: int) -> bytes:
        """Create CHAIN packet to complete handshake"""
        target_bin = TouhouProtocol.pack_sockaddr(target_addr, target_port)
        their_bin = TouhouProtocol.pack_sockaddr(their_addr, their_port)
        return bytes([PACKET_CHAIN]) + target_bin + their_bin

    @staticmethod
    def create_init_request_spectate(game_id: bytes = DEFAULT_GAME_ID) -> bytes:
        """Create INIT_REQUEST for spectating - 65 bytes fixed"""
        # For spectating: byte at offset 25 is 0x00 (vs 0x01 for playing)
        base = bytes([PACKET_INIT_REQUEST]) + game_id + STUFF_BYTES + bytes([0x00])

        # Pad to exactly 65 bytes
        padding_len = 65 - len(base)
        return base + bytes(padding_len)

    @staticmethod
    def create_init_request_response(response_data: bytes) -> bytes:
        """Create response to INIT_SUCCESS for spectating"""
        if len(response_data) < 10:
            return None
        # Copy bytes 5-8 (the spectate info) and send back as acknowledgment
        # This tells the server we're ready to receive game data
        spectate_info = response_data[5:9]
        # Send a packet with type 0x06 (INIT_SUCCESS) response format
        # The server expects this to start receiving game packets
        return bytes([PACKET_INIT_SUCCESS]) + response_data[1:5] + spectate_info

    @staticmethod
    def parse_init_success(data: bytes) -> Optional[Dict[str, Any]]:
        """Parse INIT_SUCCESS payload.

        Works with observed spectator packets where:
        - packet[0] is 0x06
        - packet[5:9] is spectate_info (u32 LE)
        - packet[9:11] is data_size (u16 LE)
        - packet[12:] starts optional data blob
        """
        if not data:
            return None

        payload = data[1:] if data[0] == PACKET_INIT_SUCCESS else data
        if len(payload) < 11:
            return None

        result: Dict[str, Any] = {
            "spectate_info": struct.unpack("<I", payload[4:8])[0],
            "data_size": struct.unpack("<H", payload[8:10])[0],
            "raw_len": len(payload),
        }

        data_offset = 11
        data_size = result["data_size"]
        data_end = min(len(payload), data_offset + data_size)
        data_blob = payload[data_offset:data_end]
        result["data_blob"] = data_blob

        # First INIT_SUCCESS commonly contains:
        # host_name_padded(32) + client_name_padded(32) + swr_disabled(4)
        if len(data_blob) >= 68:
            host_raw = data_blob[0:32]
            client_raw = data_blob[32:64]
            swr_disabled = struct.unpack("<I", data_blob[64:68])[0]

            # Try to decode player names
            # Japanese clients use Shift-JIS, Chinese clients may use GBK
            # We detect by checking if decoded GBK contains unexpected CJK characters in English names
            host_bytes = host_raw.lstrip(b"\x00").split(b"\x00", 1)[0]
            client_bytes = client_raw.lstrip(b"\x00").split(b"\x00", 1)[0]

            def _decode_name(name_bytes: bytes) -> str:
                """Decode name bytes, preferring Shift-JIS for ASCII-like content"""
                if not name_bytes:
                    return ""
                # Try Shift-JIS first (works for both English and Japanese)
                try:
                    name_sjis = name_bytes.decode("shift_jis", errors="strict")
                    # If it's mostly ASCII, Shift-JIS is correct
                    ascii_chars = sum(1 for c in name_sjis if ord(c) < 128)
                    if ascii_chars == len(name_sjis) or ascii_chars >= len(name_sjis) * 0.8:
                        return name_sjis
                except UnicodeDecodeError:
                    pass
                # Try GBK for Chinese names
                try:
                    name_gbk = name_bytes.decode("gbk", errors="strict")
                    return name_gbk
                except UnicodeDecodeError:
                    pass
                # Fallback to Shift-JIS with ignore
                return name_bytes.decode("shift_jis", errors="ignore")

            host_name = _decode_name(host_bytes)
            client_name = _decode_name(client_bytes)

            result["host_name"] = host_name
            result["client_name"] = client_name
            result["swr_disabled"] = swr_disabled

        return result

    @staticmethod
    def create_spectate_replay_request(match_id: int, frame_id: int) -> bytes:
        """Create spectate replay request - sent as CLIENT_GAME (0x0E) from spectator to parent

        Per protocol docs: GAME_REPLAY_REQUEST is sent from spectators to their parent
        to request replay data. It is always sent in CLIENT_GAME packets.
        """
        # Format: game_type (1) + frame_id (4, little endian) + match_id (1)
        game_data = struct.pack("<I", frame_id) + bytes([match_id])
        return bytes([PACKET_CLIENT_GAME, GAME_REPLAY_REQUEST]) + game_data

    @staticmethod
    def parse_host_game(data: bytes) -> Optional[Dict[str, Any]]:
        if len(data) < 2:
            return None
        game_type = data[0]
        game_data = data[1:]

        if game_type == GAME_MATCH:
            return {"type": "GAME_MATCH", "data": TouhouProtocol.parse_game_match(game_data)}
        elif game_type == GAME_REPLAY:
            return {"type": "GAME_REPLAY", "data": TouhouProtocol.parse_game_replay(game_data)}
        elif game_type == GAME_LOADED:
            return {"type": "GAME_LOADED", "scene_id": game_data[0] if game_data else 0}
        else:
            return {"type": f"UNKNOWN_{game_type:02X}", "raw": game_data.hex()}

    @staticmethod
    def parse_game_match(data: bytes) -> Optional[MatchData]:
        def _parse_player_match_data(buf: bytes, start: int) -> Optional[tuple[PlayerData, int]]:
            if start + 4 > len(buf):
                return None

            p = PlayerData()
            p.character_id = buf[start]
            p.skin_id = buf[start + 1]
            p.deck_id = buf[start + 2]
            deck_size = buf[start + 3]
            offset = start + 4

            deck_bytes = deck_size * 2
            if offset + deck_bytes + 1 > len(buf):
                return None

            p.deck = []
            for i in range(deck_size):
                card_off = offset + i * 2
                p.deck.append(struct.unpack("<H", buf[card_off:card_off + 2])[0])
            offset += deck_bytes

            p.disabled_simultaneous = buf[offset] != 0
            offset += 1
            return p, offset

        match = MatchData()
        try:
            host_parsed = _parse_player_match_data(data, 0)
            if not host_parsed:
                return None
            match.host, offset = host_parsed

            client_parsed = _parse_player_match_data(data, offset)
            if not client_parsed:
                return None
            match.client, offset = client_parsed

            # Tail: stage_id (1) + music_id (1) + random_seed (4) + match_id (1)
            if offset + 7 > len(data):
                return None

            match.stage_id = data[offset]
            match.music_id = data[offset + 1]
            match.random_seed = struct.unpack("<I", data[offset + 2:offset + 6])[0]
            match.match_id = data[offset + 6]
            return match
        except Exception:
            return None

    @staticmethod
    def parse_game_replay(data: bytes) -> Optional[Dict[str, Any]]:
        """Parse GAME_REPLAY packet containing compressed replay data"""
        # game_data = <compressed_size:1> <compressed_data:N>
        if len(data) < 2:
            return None
        try:
            # First byte is compressed data size
            compressed_size = data[0]
            payload_size = len(data) - 1
            if compressed_size == 0 or compressed_size > payload_size:
                logger.debug(f"Invalid compressed size: {compressed_size}, data len: {len(data)}")
                return None
            if compressed_size != payload_size:
                logger.debug(
                    f"GAME_REPLAY size mismatch: header={compressed_size}, payload={payload_size}; "
                    f"ignoring trailing bytes"
                )

            compressed_data = data[1:1 + compressed_size]
            if not compressed_data:
                return None

            decompressed = zlib.decompress(compressed_data)
            # Protocol docs mention an optional replay_inputs_count byte after game_inputs_count.
            # Current captures often omit it. Support both layouts.
            # replay_data = frame_id(4) + end_frame_id(4) + match_id(1) +
            #               game_inputs_count(1) + [replay_inputs_count(1)] + replay_inputs(...)
            if len(decompressed) < 10:
                logger.debug(f"Replay payload too short: {len(decompressed)}")
                return None
            result = {}
            offset = 0

            # Read header
            result["frame_id"] = struct.unpack("<I", decompressed[offset:offset + 4])[0]
            offset += 4

            result["end_frame_id"] = struct.unpack("<I", decompressed[offset:offset + 4])[0]
            offset += 4

            result["match_id"] = decompressed[offset]
            offset += 1

            result["game_inputs_count"] = decompressed[offset]
            offset += 1

            expected_pairs = result["game_inputs_count"] // 2
            if result["game_inputs_count"] % 2 != 0:
                logger.debug(f"Unexpected odd game_inputs_count={result['game_inputs_count']}")

            payload_len = len(decompressed) - offset
            replay_inputs_count = 0
            replay_parse_mode = "no_count"

            has_explicit_count = False
            if payload_len >= 1:
                possible_count = decompressed[offset]
                payload_after_count = payload_len - 1
                if payload_after_count >= 0 and payload_after_count % 4 == 0:
                    possible_pairs = payload_after_count // 4
                    if possible_count in (possible_pairs, expected_pairs):
                        has_explicit_count = True
                        replay_parse_mode = "with_count"
                        offset += 1
                        replay_inputs_count = min(possible_count, possible_pairs)

            if not has_explicit_count:
                payload_len = len(decompressed) - offset
                if payload_len % 4 != 0:
                    logger.debug(
                        f"Replay payload not aligned to input pairs: payload_len={payload_len}"
                    )
                    return None
                replay_inputs_count = payload_len // 4

            result["replay_parse_mode"] = replay_parse_mode
            result["replay_inputs_count"] = replay_inputs_count

            # Validate frame/header consistency using replay_inputs length.
            # In observed captures, frame_id behaves like a game_input word index,
            # while replay_inputs are pairs (client+host). So pair index range is
            # derived from ceil(frame_id / 2), newest first.
            if replay_inputs_count > 0:
                newest_pair_index = (result["frame_id"] + 1) // 2
                oldest_frame = newest_pair_index - replay_inputs_count + 1
                if oldest_frame <= 0:
                    logger.debug(
                        f"Invalid frame header: frame_id={result['frame_id']} cannot cover "
                        f"replay_inputs_count={replay_inputs_count}"
                    )
                    return None

            if replay_inputs_count != expected_pairs:
                logger.debug(
                    f"Replay pair count differs from expected: expected_pairs={expected_pairs}, "
                    f"using_pairs={replay_inputs_count}, mode=no_count"
                )

            # Read inputs
            result["inputs"] = []
            for i in range(replay_inputs_count):
                if offset + 4 > len(decompressed):
                    logger.debug(f"Truncated input data at input {i}")
                    break

                client_input = struct.unpack("<H", decompressed[offset:offset + 2])[0]
                host_input = struct.unpack("<H", decompressed[offset + 2:offset + 4])[0]

                # Inputs are in reverse order (newest first).
                # Convert from word-index frame_id to pair index.
                frame_num = ((result["frame_id"] + 1) // 2) - i
                result["inputs"].append({
                    "frame": frame_num,
                    "client": client_input,
                    "host": host_input
                })
                offset += 4

            logger.debug(
                f"Parsed replay: frame={result['frame_id']}, end={result['end_frame_id']}, "
                f"match={result['match_id']}, inputs={len(result['inputs'])}, mode={result['replay_parse_mode']}"
            )
            return result

        except zlib.error as e:
            logger.debug(f"Decompression error: {e}")
            return None
        except Exception as e:
            logger.debug(f"Parse error: {e}")
            return None


# Track how many lines the last status display used, for clearing.
_last_status_lines = 0


def print_status_display():
    """Print a multi-line live status display showing all active captures.
    Uses cursor movement to update in place, compatible with Windows Terminal.
    """
    global _last_status_lines
    # Build new output lines
    lines: List[str] = []
    lines.append(stats.get_status_line())
    active = stats.get_active_games_snapshot()
    if active:
        lines.append(f"{'IP':<24} {'Host':<20} {'Client':<20} {'Match':>5} {'Frames':>7} {'Saved':>5} {'Status'}")
        lines.append("-" * 96)
        for g in active:
            h_display = f"{g.host_name}({g.host_char})" if g.host_char else g.host_name
            c_display = f"{g.client_name}({g.client_char})" if g.client_char else g.client_name
            # Truncate long names to prevent line overflow
            h_display = (h_display[:17] + "...") if len(h_display) > 20 else h_display
            c_display = (c_display[:17] + "...") if len(c_display) > 20 else c_display
            lines.append(
                f"{g.ip:<24} {h_display:<20} {c_display:<20} {g.match_id:>5} {g.frame_count:>7} {g.saved_count:>5} {g.status}"
            )
    else:
        lines.append("No active captures")

    # Move cursor up to the start of previous display
    if _last_status_lines > 0:
        # \033[{n}A = move up n lines, then \r = return to start of line
        sys.stdout.write(f"\033[{_last_status_lines}A")

    # Print each line, clearing to end of line
    for i, line in enumerate(lines):
        if i > 0:
            sys.stdout.write("\n")
        # \r = return to start, \033[K = clear to end of line
        sys.stdout.write(f"\r\033[K{line}")

    # If previous display had more lines, clear the remaining ones
    if _last_status_lines > len(lines):
        for _ in range(_last_status_lines - len(lines)):
            sys.stdout.write("\n\r\033[K")

    sys.stdout.flush()
    _last_status_lines = len(lines)


def print_status_line():
    """Print a single status line that updates in place (legacy)"""
    print_status_display()


class SpectatorClient:
    """Spectator client for capturing replays"""

    def __init__(self, host: str, port: int, game_info: Dict):
        self.host = host
        self.port = port
        self.game_info = game_info
        self.sock: Optional[socket.socket] = None
        self.replay_data = ReplayData()
        self.init_success_info: Optional[Dict[str, Any]] = None
        self.selected_game_id: bytes = DEFAULT_GAME_ID
        self.running = False
        self.filepath = ""
        self.ip_port = f"{host}:{port}"

    def connect(self) -> bool:
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.settimeout(10.0)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # Don't connect - use sendto/recvfrom for UDP hole punching
            local_addr = ("0.0.0.0", 0)
            self.sock.bind(local_addr)
            local_port = self.sock.getsockname()[1]
            logger.info(f"[{self.ip_port}] Local socket bound to port {local_port}")

            # Step 1: Send HELLO and wait for OLLEH
            hello_packet = TouhouProtocol.create_hello(self.host, self.port)
            logger.info(f"[{self.ip_port}] Sending HELLO ({len(hello_packet)} bytes)...")
            for i in range(5):
                self.sock.sendto(hello_packet, (self.host, self.port))
                logger.info(f"[{self.ip_port}] HELLO sent (attempt {i+1}/5)")
                time.sleep(0.1)

            # Wait for OLLEH (just 1 byte: 0x03)
            got_olleh = False
            logger.info(f"[{self.ip_port}] Waiting for OLLEH...")
            for attempt in range(30):  # 3 seconds timeout
                try:
                    self.sock.settimeout(0.1)
                    data, addr = self.sock.recvfrom(4096)
                    if data:
                        logger.info(f"[{self.ip_port}] Received packet ({len(data)} bytes): type={data[0]:02X}")
                        if data[0] == PACKET_OLLEH:
                            logger.info(f"[{self.ip_port}] Got OLLEH from {addr}")
                            got_olleh = True
                            break
                except socket.timeout:
                    continue

            if not got_olleh:
                logger.warning(f"[{self.ip_port}] No OLLEH received after 30 attempts")
                return False

            # Step 2: Send INIT_REQUEST (no CHAIN needed for spectating).
            # Auto-detect the compatible session by trying known spectator game IDs.
            init_response = None
            logger.info(f"[{self.ip_port}] Waiting for INIT_SUCCESS...")
            for candidate_game_id in SPECTATE_GAME_ID_CANDIDATES:
                init_packet = TouhouProtocol.create_init_request_spectate(candidate_game_id)
                logger.info(
                    f"[{self.ip_port}] Sending INIT_REQUEST ({len(init_packet)} bytes) with game_id={candidate_game_id.hex()}..."
                )
                for _ in range(2):
                    self.sock.sendto(init_packet, (self.host, self.port))
                    time.sleep(0.05)

                candidate_deadline = time.time() + 1.2
                while time.time() < candidate_deadline:
                    try:
                        self.sock.settimeout(0.1)
                        data, addr = self.sock.recvfrom(4096)
                        if data:
                            logger.info(f"[{self.ip_port}] Received packet ({len(data)} bytes): type={data[0]:02X}")
                            if data[0] == PACKET_INIT_SUCCESS:
                                init_response = data
                                self.selected_game_id = candidate_game_id
                                logger.info(
                                    f"[{self.ip_port}] Got INIT_SUCCESS: {len(data)} bytes with game_id={candidate_game_id.hex()}"
                                )
                                break
                            if data[0] == PACKET_INIT_ERROR:
                                logger.warning(f"[{self.ip_port}] INIT_ERROR received")
                                return False
                    except socket.timeout:
                        continue

                if init_response:
                    break

            if not init_response:
                logger.warning(f"[{self.ip_port}] No INIT_SUCCESS received for any known game_id")
                return False

            self.init_success_info = TouhouProtocol.parse_init_success(init_response)
            if self.init_success_info:
                self.replay_data.debug_init_spectate_info = int(self.init_success_info.get("spectate_info", 0))
                self.replay_data.debug_init_host_name = str(self.init_success_info.get("host_name", ""))
                self.replay_data.debug_init_client_name = str(self.init_success_info.get("client_name", ""))
                self.replay_data.debug_init_swr_disabled = int(self.init_success_info.get("swr_disabled", 0))
                self.replay_data.debug_init_game_id = self.selected_game_id.hex()
                logger.debug(
                    f"[{self.ip_port}] INIT_SUCCESS parsed: spectate_info={self.replay_data.debug_init_spectate_info} "
                    f"host='{self.replay_data.debug_init_host_name}' "
                    f"client='{self.replay_data.debug_init_client_name}' "
                    f"swr_disabled={self.replay_data.debug_init_swr_disabled} "
                    f"game_id={self.replay_data.debug_init_game_id}"
                )

            # Now connected - switch to connected mode
            logger.info(f"[{self.ip_port}] Handshake complete, switching to connected mode")
            self.sock.connect((self.host, self.port))
            self.sock.settimeout(2.0)

            return True

        except Exception as e:
            log_error(f"Connection to {self.ip_port}", e, {
                "host": self.host,
                "port": self.port,
                "game_info": self.game_info
            })
            return False

    def _receive_packet(self, timeout: float = 0.05) -> Optional[bytes]:
        try:
            # Keep recv non-blocking-ish so request cadence is not starved by long waits.
            self.sock.settimeout(timeout)
            data = self.sock.recv(4096)
            return data
        except socket.timeout:
            return None
        except Exception as e:
            logger.debug(f"Receive error: {e}")
            return None

    def capture_replay(
        self,
        duration: Optional[float] = 60.0,
        ignore_game_replay: bool = False,
        stop_on_match_end: bool = False,
        stop_immediately_on_match_end: bool = False,
        stop_on_match_switch: bool = False,
        stop_on_match_quiet: bool = False,
        match_quiet_timeout: float = 2.5,
        idle_timeout_after_end: float = 5.0,
        idle_timeout_no_data: Optional[float] = 30.0,
        stop_on_no_progress: bool = False,
        no_progress_timeout: float = 10.0,
        stop_when_never_readable: bool = False,
        never_readable_timeout: float = 20.0,
        stop_on_empty_replay_marker: bool = False,
        empty_replay_marker_threshold: int = 20,
        dump_stream: bool = False,
        dump_stream_file: Optional[str] = None,
        dump_max_hex_bytes: int = 96,
    ) -> ReplayData:
        self.running = True
        start_time = time.time()
        current_match_id = 0
        frame_inputs_by_id: Dict[int, FrameInput] = {}
        raw_words_seen: Set[int] = set()
        # Match real client behavior observed in pcap:
        # send an initial probe request with frame_id=0xFFFFFFFF, then switch to 0.
        requested_frame = 0xFFFFFFFF
        sent_initial_probe = False
        contiguous_word_frontier = 0
        last_sent_request_frame = 0xFFFFFFFF
        current_match_seen_at: Optional[float] = None
        initial_probe_send_count = 0
        last_status_update = 0
        last_request_time = 0
        last_packet_time = start_time
        match_end_detected_at: Optional[float] = None
        # Protocol says spectators send GAME_REPLAY_REQUEST every 3 frames.
        # At 60fps this is approximately 50ms.
        request_interval = 0.05
        initial_probe_repeat_target = 4
        bootstrap_match_hold_interval = 0.30
        end_frame_id = 0
        empty_end_marker_seen_at: Optional[float] = None
        empty_end_marker_max_word = 0
        last_replay_frame_seen = 0
        last_replay_data_frame_seen = 0
        max_replay_word_seen = 0
        latest_replay_word_count = 0
        latest_replay_first_word = 0
        active_match_id: Optional[int] = None
        seen_replay_for_active_match = False
        last_progress_time = start_time
        has_real_replay_progress = False
        has_readable_session_signal = False
        has_seen_match_packet = False
        has_seen_replay_packet = False
        last_replay_packet_time: Optional[float] = None
        last_match_packet_time: Optional[float] = None
        empty_replay_streak = 0
        stop_reason = ""
        dump_fh = None
        last_missing_report_time = 0.0
        last_missing_report_max_frame = 0

        def _missing_frames_upto(frame_limit: int) -> List[int]:
            if frame_limit <= 0:
                return []
            return [f for f in range(1, frame_limit + 1) if f not in frame_inputs_by_id]

        def _format_missing_ranges(missing_frames: List[int], max_ranges: int = 32) -> str:
            if not missing_frames:
                return "none"

            ranges: List[str] = []
            start = prev = missing_frames[0]
            for frame_id in missing_frames[1:]:
                if frame_id == prev + 1:
                    prev = frame_id
                    continue
                ranges.append(f"{start}" if start == prev else f"{start}-{prev}")
                start = prev = frame_id
            ranges.append(f"{start}" if start == prev else f"{start}-{prev}")

            if len(ranges) > max_ranges:
                shown = ", ".join(ranges[:max_ranges])
                return f"{shown}, ... (+{len(ranges) - max_ranges} ranges)"
            return ", ".join(ranges)

        if dump_stream and dump_stream_file:
            try:
                os.makedirs(os.path.dirname(dump_stream_file) or ".", exist_ok=True)
                dump_fh = open(dump_stream_file, "a", encoding="utf-8")
                dump_fh.write(f"# stream dump start {datetime.now().isoformat()} {self.ip_port}\n")
                dump_fh.flush()
            except Exception as e:
                logger.warning(f"[{self.ip_port}] Failed to open stream dump file: {e}")
                dump_fh = None

        def _dump_packet(direction: str, packet: bytes):
            if not dump_stream:
                return
            size = len(packet)
            shown = packet[:dump_max_hex_bytes]
            hex_data = binascii.hexlify(shown).decode("ascii")
            suffix = "" if size <= dump_max_hex_bytes else "..."
            line = f"{time.time():.6f} {direction} len={size} hex={hex_data}{suffix}"
            if dump_fh:
                dump_fh.write(line + "\n")
                dump_fh.flush()

        def _has_time_left(now: float) -> bool:
            return duration is None or (now - start_time) < duration

        def _max_captured_frame() -> int:
            if frame_inputs_by_id:
                return max(frame_inputs_by_id.keys())
            if not self.replay_data.frames:
                return -1
            return max(frame.frame_id for frame in self.replay_data.frames)

        def _choose_request_frame(now: float) -> int:
            """Match official client behavior exactly:
            - Phase 1: 0xFFFFFFFF (initial probe)
            - Phase 2: 0 (bootstrap, waiting for GAME_MATCH)
            - Phase 3+: requested_frame (updated by REPLAY handler as max(requested_frame, reply.frame_id))
            """
            if not sent_initial_probe:
                return 0xFFFFFFFF

            if current_match_id == 0:
                return 0 if requested_frame == 0xFFFFFFFF else requested_frame

            waiting_for_first_replay = (
                active_match_id is not None
                and current_match_id == active_match_id
                and not seen_replay_for_active_match
            )

            if waiting_for_first_replay:
                recent_match_signal = (
                    last_match_packet_time is not None
                    and (now - last_match_packet_time) < bootstrap_match_hold_interval
                )
                if recent_match_signal or (
                    current_match_seen_at is not None
                    and (now - current_match_seen_at) < bootstrap_match_hold_interval
                ):
                    return 0

            return 0 if requested_frame == 0xFFFFFFFF else requested_frame

        while self.running and _has_time_left(time.time()):
            current_time = time.time()

            # Optional guard for network silence to avoid waiting forever.
            if idle_timeout_no_data is not None and (current_time - last_packet_time) > idle_timeout_no_data:
                logger.info(f"[{self.ip_port}] Stopping due to no packets for {idle_timeout_no_data:.1f}s")
                stop_reason = "idle_timeout_no_data"
                break

            # Send replay request at intervals (every 3 frames as per protocol)
            if current_time - last_request_time > request_interval:
                send_request_frame = _choose_request_frame(current_time)
                replay_req = TouhouProtocol.create_spectate_replay_request(current_match_id, send_request_frame)
                try:
                    self.sock.send(replay_req)
                    _dump_packet("SEND", replay_req)
                    logger.debug(
                        f"[{self.ip_port}] SEND GAME_REPLAY_REQUEST frame_id={send_request_frame} "
                        f"match_id={current_match_id} raw={replay_req.hex()}"
                    )
                    last_sent_request_frame = send_request_frame
                    if not sent_initial_probe and send_request_frame == 0xFFFFFFFF:
                        initial_probe_send_count += 1
                        if initial_probe_send_count >= initial_probe_repeat_target:
                            sent_initial_probe = True
                            requested_frame = 0
                            last_sent_request_frame = 0
                    last_request_time = current_time
                except Exception as e:
                    logger.debug(f"Send error: {e}")
                    stop_reason = "send_error"
                    break

            # Drain all available packets from socket before sending next request.
            # The server responds with multiple packets per request (backfill chunks);
            # processing only one per loop causes the receive buffer to grow unbounded.
            first_recv = True
            while True:
                response = self._receive_packet(timeout=0.05 if first_recv else 0.0)
                first_recv = False
                if not response:
                    break
                current_time = time.time()
                last_packet_time = current_time
                _dump_packet("RECV", response)
                packet_type = response[0]
                if packet_type == PACKET_HOST_GAME and len(response) > 1:
                    game_subtype = response[1]
                    game_data = response[2:]

                    if game_subtype == GAME_MATCH:
                        logger.debug(
                            f"[{self.ip_port}] RECV GAME_MATCH len={len(game_data)} "
                            f"raw={game_data.hex()}"
                        )
                        match_data = TouhouProtocol.parse_game_match(game_data)
                        if match_data:
                            self.replay_data.debug_session_signals += 1
                            self.replay_data.debug_match_packets += 1
                            has_readable_session_signal = True
                            has_seen_match_packet = True
                            last_match_packet_time = current_time
                            is_new_match = active_match_id is None or match_data.match_id != active_match_id
                            # If a new match starts, the previous one has ended.
                            if (
                                stop_on_match_end
                                and stop_on_match_switch
                                and active_match_id is not None
                                and seen_replay_for_active_match
                                and match_data.match_id != active_match_id
                            ):
                                logger.info(
                                    f"[{self.ip_port}] Match switched {active_match_id} -> {match_data.match_id}, stopping capture"
                                )
                                # Keep old match_data — frames belong to the previous match.
                                self.replay_data.ended = True
                                stop_reason = "match_switch"
                                break

                            self.replay_data.match_data = match_data
                            current_match_id = match_data.match_id
                            if is_new_match:
                                active_match_id = current_match_id
                                seen_replay_for_active_match = False
                                current_match_seen_at = current_time
                                # Only reset offset when a genuinely new match is observed.
                                raw_words_seen.clear()
                                contiguous_word_frontier = 0
                                last_sent_request_frame = 0
                                requested_frame = 0
                                max_replay_word_seen = 0
                                latest_replay_word_count = 0
                                latest_replay_first_word = 0
                            logger.debug(
                                f"[{self.ip_port}] Parsed GAME_MATCH match_id={current_match_id} "
                                f"stage={match_data.stage_id} music={match_data.music_id} "
                                f"chars={match_data.host.character_id}v{match_data.client.character_id} "
                                f"requested_frame={requested_frame}"
                            )
                            # Spectators should keep polling with GAME_REPLAY_REQUEST.
                            # GAME_MATCH_ACK is used for GAME_MATCH_REQUEST between player peers.
                        else:
                            self.replay_data.debug_match_parse_failures += 1
                            logger.debug(
                                f"[{self.ip_port}] Failed to parse GAME_MATCH payload len={len(game_data)} "
                                f"raw={game_data.hex()}"
                            )

                    elif game_subtype == GAME_REPLAY:
                        if ignore_game_replay:
                            logger.debug(f"[{self.ip_port}] Ignoring GAME_REPLAY (0x0D/0x09) by debug option")
                            continue
                        replay_info = TouhouProtocol.parse_game_replay(game_data)
                        if replay_info:
                            self.replay_data.debug_session_signals += 1
                            self.replay_data.debug_replay_packets += 1
                            has_readable_session_signal = True
                            has_seen_replay_packet = True
                            last_replay_packet_time = current_time
                            raw_frame_id = int(replay_info.get("frame_id", 0))
                            raw_game_inputs_count = int(replay_info.get("game_inputs_count", 0))
                            raw_first_word = raw_frame_id - raw_game_inputs_count + 1 if raw_game_inputs_count > 0 else raw_frame_id
                            latest_replay_word_count = raw_game_inputs_count
                            latest_replay_first_word = raw_first_word

                            if raw_game_inputs_count > 0:
                                if max_replay_word_seen > 0 and raw_first_word > max_replay_word_seen + 1:
                                    logger.debug(
                                        f"[{self.ip_port}] Replay gap before backfill: "
                                        f"missing_words={max_replay_word_seen + 1}-{raw_first_word - 1} "
                                        f"incoming_words={raw_first_word}-{raw_frame_id}"
                                    )
                                if max_replay_word_seen > 0 and raw_frame_id < max_replay_word_seen:
                                    logger.debug(
                                        f"[{self.ip_port}] Replay out-of-order/backfill packet: "
                                        f"incoming_words={raw_first_word}-{raw_frame_id} after_seen={max_replay_word_seen}"
                                    )
                                if raw_frame_id > max_replay_word_seen:
                                    max_replay_word_seen = raw_frame_id

                                for raw_word in range(raw_first_word, raw_frame_id + 1):
                                    raw_words_seen.add(raw_word)

                                while (contiguous_word_frontier + 1) in raw_words_seen:
                                    contiguous_word_frontier += 1

                                # 官方客户端策略: requested_frame = max(requested_frame, reply.frame_id)
                                if sent_initial_probe and raw_frame_id > requested_frame:
                                    logger.debug(
                                        f"[{self.ip_port}] requested_frame {requested_frame} -> {raw_frame_id} "
                                        f"(replay-driven, match={current_match_id}, frontier={contiguous_word_frontier}, "
                                        f"max_seen={max_replay_word_seen}, words={raw_game_inputs_count})"
                                    )
                                    requested_frame = raw_frame_id

                            is_empty_replay_marker = (
                                replay_info.get("frame_id", 0) == 0
                                and replay_info.get("end_frame_id", 0) == 0
                                and replay_info.get("match_id", 0) == 0
                                and replay_info.get("game_inputs_count", 0) == 0
                                and not replay_info.get("inputs")
                            )
                            if is_empty_replay_marker:
                                empty_replay_streak += 1
                            else:
                                empty_replay_streak = 0

                            if (
                                stop_on_match_end
                                and stop_on_empty_replay_marker
                                and has_real_replay_progress
                                and empty_replay_streak >= empty_replay_marker_threshold
                            ):
                                logger.info(
                                    f"[{self.ip_port}] Empty replay marker streak={empty_replay_streak}, stopping capture"
                                )
                                self.replay_data.ended = True
                                stop_reason = "empty_replay_marker"
                                break

                            # Check match_id
                            replay_match_id = replay_info.get("match_id", 0)
                            if replay_match_id != current_match_id:
                                logger.debug(f"Match ID changed: {current_match_id} -> {replay_match_id}")
                                current_match_id = replay_match_id
                            if active_match_id is None:
                                active_match_id = current_match_id
                            if current_match_id == active_match_id:
                                seen_replay_for_active_match = True

                            # Add inputs while handling duplicate and out-of-order frames.
                            new_inputs = replay_info.get("inputs", [])
                            made_progress = False
                            for inp in new_inputs:
                                frame_id = int(inp["frame"])
                                if frame_id <= 0:
                                    continue

                                incoming = FrameInput(
                                    frame_id=frame_id,
                                    client_input=int(inp["client"]),
                                    host_input=int(inp["host"]),
                                )
                                previous = frame_inputs_by_id.get(frame_id)
                                if previous is None:
                                    frame_inputs_by_id[frame_id] = incoming
                                    made_progress = True
                                elif (
                                    previous.client_input != incoming.client_input
                                    or previous.host_input != incoming.host_input
                                ):
                                    frame_inputs_by_id[frame_id] = incoming

                            if made_progress:
                                last_progress_time = current_time
                                has_real_replay_progress = True

                            # Keep a live copy for status counters; final save normalizes/sorts again.
                            self.replay_data.frames = list(frame_inputs_by_id.values())

                            if replay_info["frame_id"] > 0:
                                last_progress_time = current_time
                                has_real_replay_progress = True
                                if replay_info["frame_id"] > last_replay_frame_seen:
                                    last_replay_frame_seen = replay_info["frame_id"]
                                if replay_info.get("inputs") and replay_info["frame_id"] > last_replay_data_frame_seen:
                                    last_replay_data_frame_seen = replay_info["frame_id"]

                            # Runtime missing-frame report (throttled) so gaps are visible during capture.
                            current_max_frame = _max_captured_frame()
                            if (
                                current_max_frame > 0
                                and (
                                    current_time - last_missing_report_time >= 1.0
                                    or current_max_frame - last_missing_report_max_frame >= 200
                                )
                            ):
                                missing_now = _missing_frames_upto(current_max_frame)
                                # if missing_now:
                                #     logger.warning(
                                #         f"[{self.ip_port}] Missing frame inputs up to {current_max_frame}: "
                                #         f"count={len(missing_now)} ranges={_format_missing_ranges(missing_now)}"
                                #     )
                                # else:
                                #     logger.debug(
                                #         f"[{self.ip_port}] No missing frame inputs up to {current_max_frame}"
                                #     )
                                last_missing_report_time = current_time
                                last_missing_report_max_frame = current_max_frame

                            # Check if match ended
                            if replay_info.get("end_frame_id", 0) > 0:
                                raw_frame_id = replay_info.get("frame_id", 0)
                                raw_end_frame_id = replay_info["end_frame_id"]
                                raw_announced_end_word = max(raw_frame_id, raw_end_frame_id)
                                pair_end_frame = (raw_announced_end_word + 1) // 2
                                self.replay_data.ended = True
                                self.replay_data.end_frame = max(self.replay_data.end_frame, pair_end_frame)
                                end_frame_id = max(end_frame_id, raw_announced_end_word)
                                if match_end_detected_at is None:
                                    match_end_detected_at = current_time
                                logger.debug(
                                    f"Match end announced at raw_frame={raw_frame_id}, raw_end_frame={raw_end_frame_id}, "
                                    f"raw_announced_end={raw_announced_end_word}, pair_end_frame={pair_end_frame}"
                                )

                                if (
                                    replay_info.get("game_inputs_count", 0) == 0
                                    and not replay_info.get("inputs")
                                ):
                                    empty_end_marker_seen_at = current_time
                                    empty_end_marker_max_word = max(
                                        empty_end_marker_max_word,
                                        raw_announced_end_word,
                                    )
                                    logger.debug(
                                        f"[{self.ip_port}] Empty end marker received: raw_frame={raw_frame_id}, "
                                        f"raw_end_frame={raw_end_frame_id}, frontier={contiguous_word_frontier}, "
                                        f"last_data_frame={last_replay_data_frame_seen}"
                                    )

                                if stop_on_match_end and stop_immediately_on_match_end:
                                    logger.info(
                                        f"[{self.ip_port}] Match end announced at raw_end_frame={end_frame_id}, stopping immediately"
                                    )
                                    stop_reason = "end_frame_immediate"
                                    break
                        else:
                            self.replay_data.debug_replay_parse_failures += 1

                    elif game_subtype == GAME_LOADED:
                        scene_id = game_data[0] if game_data else 0
                        logger.debug(f"Game loaded, scene: {scene_id}")
                        # Send LOADED_ACK
                        try:
                            ack = bytes([PACKET_CLIENT_GAME, GAME_LOADED_ACK, scene_id])
                            self.sock.send(ack)
                        except:
                            pass

            # If packet processing set stop_reason (match_switch, end_frame, etc.), break outer loop
            if stop_reason:
                break

            # Stop based on match end signal instead of fixed duration.
            if stop_on_match_end and end_frame_id > 0:
                if (
                    empty_end_marker_seen_at is not None
                    and contiguous_word_frontier >= max(end_frame_id, empty_end_marker_max_word)
                    and (current_time - empty_end_marker_seen_at) > 0.30
                ):
                    logger.info(
                        f"[{self.ip_port}] End marker settled with contiguous frontier={contiguous_word_frontier}, "
                        f"announced_end={max(end_frame_id, empty_end_marker_max_word)}, stopping capture"
                    )
                    stop_reason = "end_marker_settled"
                    break

                if match_end_detected_at is not None and (current_time - match_end_detected_at) > idle_timeout_after_end:
                    logger.info(
                        f"[{self.ip_port}] End frame {end_frame_id} announced, stopping after {idle_timeout_after_end:.1f}s grace "
                        f"(last data frame={last_replay_data_frame_seen})"
                    )
                    stop_reason = "end_frame_grace_timeout"
                    break

            # Primary lifecycle fallback: GAME_MATCH stops arriving while session is still alive.
            if (
                stop_on_match_end
                and stop_on_match_quiet
                and has_seen_match_packet
                and has_seen_replay_packet
                and last_match_packet_time is not None
                and (current_time - last_match_packet_time) > match_quiet_timeout
                and (current_time - last_packet_time) < match_quiet_timeout
            ):
                logger.info(
                    f"[{self.ip_port}] GAME_MATCH quiet for {match_quiet_timeout:.1f}s while session alive, stopping capture"
                )
                self.replay_data.ended = True
                stop_reason = "match_quiet_timeout"
                break

            # Fallback for sessions that never emit explicit end markers.
            if (
                stop_on_match_end
                and stop_on_no_progress
                and has_real_replay_progress
                and (current_time - last_progress_time) > no_progress_timeout
            ):
                logger.info(
                    f"[{self.ip_port}] No replay progress for {no_progress_timeout:.1f}s, stopping capture "
                    f"(requested_frame={requested_frame}, frames={len(self.replay_data.frames)})"
                )
                stop_reason = "no_replay_progress"
                break

            # If we never see any meaningful replay progress, treat this target as unreadable.
            if (
                stop_when_never_readable
                and not has_readable_session_signal
                and (current_time - start_time) > never_readable_timeout
            ):
                logger.info(
                    f"[{self.ip_port}] No readable session packets for {never_readable_timeout:.1f}s, giving up this target"
                )
                stop_reason = "never_readable_timeout"
                break

            # Update status display
            if current_time - last_status_update > 1.0:
                elapsed = int(current_time - start_time)
                frames = len(self.replay_data.frames)
                if duration is None:
                    status_text = f"{elapsed}s, {frames}f"
                else:
                    status_text = f"{elapsed}s/{int(duration)}s, {frames}f"
                stats.set_game_status(self.ip_port, status_text)
                # Update match/frame/char info for live display
                h_char = ""
                c_char = ""
                if self.replay_data.match_data:
                    h_char = CHARACTERS.get(self.replay_data.match_data.host.character_id, "unk")
                    c_char = CHARACTERS.get(self.replay_data.match_data.client.character_id, "unk")
                stats.update_game_capture(
                    self.ip_port, current_match_id, frames,
                    host_char=h_char, client_char=c_char,
                )
                last_status_update = current_time

            time.sleep(0.016)  # ~60fps loop

        if not stop_reason:
            if duration is not None and (time.time() - start_time) >= duration:
                stop_reason = "duration_reached"
            elif not self.running:
                stop_reason = "stopped"
            else:
                stop_reason = "loop_exit"
        self.replay_data.debug_stop_reason = stop_reason
        self.replay_data.debug_last_replay_frame = last_replay_frame_seen
        self.replay_data.debug_last_requested_frame = last_sent_request_frame
        self.replay_data.debug_last_match_id = current_match_id
        self.replay_data.frames = normalize_frame_inputs(self.replay_data.frames)
        logger.info(
            f"[{self.ip_port}] Capture ended: reason={stop_reason}, frames={len(self.replay_data.frames)}, "
            f"session_packets={self.replay_data.debug_session_signals}, "
            f"match_packets={self.replay_data.debug_match_packets}, replay_packets={self.replay_data.debug_replay_packets}, "
            f"match_parse_failures={self.replay_data.debug_match_parse_failures}, "
            f"replay_parse_failures={self.replay_data.debug_replay_parse_failures}, "
            f"last_replay_frame={last_replay_frame_seen}, requested_frame={requested_frame}, match_id={current_match_id}"
        )
        final_max_frame = _max_captured_frame()
        final_missing = _missing_frames_upto(final_max_frame)
        # if final_missing:
        #     logger.warning(
        #         f"[{self.ip_port}] Final missing frame inputs up to {final_max_frame}: "
        #         f"count={len(final_missing)} ranges={_format_missing_ranges(final_missing, max_ranges=64)}"
        #     )
        # else:
        #     logger.info(f"[{self.ip_port}] Final missing frame inputs: none (up to {final_max_frame})")

        if self.replay_data.end_frame > 0:
            missing_to_end = _missing_frames_upto(self.replay_data.end_frame)
            # if missing_to_end:
            #     logger.warning(
            #         f"[{self.ip_port}] Missing frame inputs up to end_frame={self.replay_data.end_frame}: "
            #         f"count={len(missing_to_end)} ranges={_format_missing_ranges(missing_to_end, max_ranges=64)}"
            #     )
            # else:
            #     logger.info(f"[{self.ip_port}] No missing frame inputs up to end_frame={self.replay_data.end_frame}")

        if dump_fh:
            try:
                dump_fh.write(f"# stream dump end {datetime.now().isoformat()} {self.ip_port}\n")
                dump_fh.write(
                    f"# summary reason={stop_reason} frames={len(self.replay_data.frames)} "
                    f"session_packets={self.replay_data.debug_session_signals} "
                    f"match_packets={self.replay_data.debug_match_packets} "
                    f"replay_packets={self.replay_data.debug_replay_packets} "
                    f"match_parse_failures={self.replay_data.debug_match_parse_failures} "
                    f"replay_parse_failures={self.replay_data.debug_replay_parse_failures} "
                    f"last_replay_frame={last_replay_frame_seen} "
                    f"requested_frame={requested_frame} "
                    f"match_id={current_match_id}\n"
                )
                dump_fh.close()
            except Exception:
                pass
        return self.replay_data

    def close(self):
        if self.sock:
            try:
                self.sock.send(bytes([PACKET_QUIT]))
            except:
                pass
            self.sock.close()
            self.sock = None
            self.sock = None


def save_replay_simple(replay: ReplayData, filepath: str) -> bool:
    """Save replay in client-like .rep layout (Rinkastone offsets + observed constants)."""
    replay.frames = normalize_frame_inputs(replay.frames)
    if not replay.frames:
        return False

    try:
        def _player_block(player: Optional[PlayerData], side_marker: int) -> bytes:
            if player is None:
                char_id = 0
                skin_id = 0
                deck_cards: List[int] = []
                disabled_sim = 0
            else:
                char_id = int(player.character_id) & 0xFF
                skin_id = int(player.skin_id) & 0xFF
                deck_cards = [int(card) & 0xFFFF for card in player.deck]
                disabled_sim = 1 if player.disabled_simultaneous else 0

            # Client files reserve 20 deck slots in this section.
            deck_count = min(len(deck_cards), 20)
            deck_cards = deck_cards[:20] + [0] * max(0, 20 - len(deck_cards))

            buf = bytearray()
            buf.append(char_id)
            buf.append(skin_id)
            buf.extend(struct.pack("<I", deck_count))
            for card in deck_cards:
                buf.extend(struct.pack("<H", card))
            buf.append(side_marker & 0xFF)  # 0x3C / 0x6E
            buf.append(0x00)  # 0x3D / 0x6F observed as 0x00 in paired sample
            buf.append(disabled_sim & 0xFF)  # 0x3E / 0x70 stores disabled_simultaneous in paired sample
            return bytes(buf)

        os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else ".", exist_ok=True)
        with open(filepath, "wb") as f:
            # 0x0000-0x000D
            header = bytearray()
            header.extend(bytes([0xD2, 0x00]))  # version marker (1.10)
            header.extend(bytes([0x00, 0x00, 0x00, 0x00]))
            header.extend(bytes([0x03, 0x09]))
            header.extend(bytes([0x06]))  # observed mode marker in client-generated pvp reps
            header.extend(bytes([0x00, 0x00, 0x01, 0x03, 0x00]))

            if replay.match_data:
                host_player = replay.match_data.host
                client_player = replay.match_data.client
                stage_id = int(replay.match_data.stage_id) & 0xFF
                music_id = int(replay.match_data.music_id) & 0xFF
                random_seed = int(replay.match_data.random_seed) & 0xFFFFFFFF
            else:
                host_player = PlayerData(character_id=0, skin_id=0)
                client_player = PlayerData(character_id=0, skin_id=0)
                stage_id = 0
                music_id = 0
                random_seed = 0

            header.extend(_player_block(host_player, side_marker=0x00))
            header.extend(_player_block(client_player, side_marker=0x01))

            # 0x0071+ fields in article/observed files.
            header.extend(bytes([0x03]))
            header.extend(bytes([stage_id, music_id]))
            header.extend(struct.pack("<I", random_seed))

            # Build a contiguous timeline: one input pair per frame.
            # Missing frames are zero-filled to keep replay duration consistent with end_frame/highest frame.
            frame_map: Dict[int, FrameInput] = {frame.frame_id: frame for frame in replay.frames}
            highest_captured = max(frame_map.keys()) if frame_map else 0
            end_frame = int(replay.end_frame) if replay.end_frame and replay.end_frame > 0 else highest_captured
            total_frames = max(end_frame, highest_captured)

            input_words: List[int] = []
            for frame_id in range(1, total_frames + 1):
                frame = frame_map.get(frame_id)
                if frame is None:
                    first_input = 0
                    second_input = 0
                else:
                    # Real .rep word stream stores host-side input first, then client-side input.
                    # This mapping is verified against test/new.pcapng <-> test/*.rep.
                    first_input = int(frame.host_input) & 0xFFFF
                    second_input = int(frame.client_input) & 0xFFFF
                input_words.append(first_input)
                input_words.append(second_input)

            # 0x77-0x7A stores game_input entry count (both players combined), not frame count.
            # For paired test samples this is exactly total_frames * 2.
            game_input_count = total_frames * 2
            if len(input_words) != game_input_count:
                logger.warning(
                    "Replay input count mismatch while saving: words=%d expected=%d total_frames=%d",
                    len(input_words),
                    game_input_count,
                    total_frames,
                )
                game_input_count = len(input_words)

            header.extend(struct.pack("<I", game_input_count))
            f.write(header)

            for word in input_words:
                f.write(struct.pack("<H", word))

        return True
    except Exception as e:
        log_error(f"Failed to save replay to {filepath}", e, {
            "filepath": filepath,
            "frames_count": len(replay.frames) if replay.frames else 0,
            "match_data": replay.match_data is not None
        })
        return False


def fetch_games() -> List[Dict[str, Any]]:
    """Fetch available games from server"""
    try:
        logger.info(f"Fetching games from {GAMES_URL}...")
        import urllib.request
        req = urllib.request.Request(GAMES_URL)
        req.add_header('User-Agent', 'Mozilla/5.0')
        # Prevent caching - always get fresh data
        req.add_header('Cache-Control', 'no-cache, no-store, must-revalidate')
        req.add_header('Pragma', 'no-cache')
        req.add_header('Expires', '0')
        with urllib.request.urlopen(req, timeout=10) as response:
            data = response.read()
            games = json.loads(data)
            logger.info(f"Fetched {len(games)} total games from server")
            return games
    except Exception as e:
        log_error("Failed to fetch games from server", e, {"url": GAMES_URL})
        return []


def _sanitize_name(name: str, fallback: str) -> str:
    safe = (name or fallback).replace("/", "_").replace("\\", "_").replace(":", "_")
    return safe[:20]


def _build_session_dir(game_info: Dict[str, Any], output_dir: str,
                        real_host_name: str = None, real_client_name: str = None) -> str:
    """Build a session directory for a persistent game session.
    Format: {host_name}-{client_name}_{ip}_{port}
    Uses real names from INIT_SUCCESS packet if available, otherwise falls back to API names.
    """
    # Prefer real names from packet, fallback to API names
    host_name = _sanitize_name(real_host_name or game_info.get("host_name", "host"), "host")
    client_name = _sanitize_name(real_client_name or game_info.get("client_name", "client"), "client")
    ip_port = game_info.get("ip", "unknown")
    safe_ip_port = ip_port.replace(":", "_").replace(".", "_")
    dir_name = f"{host_name}-{client_name}_{safe_ip_port}"
    return os.path.join(output_dir, dir_name)


def _build_replay_filename(replay: ReplayData, game_info: Dict[str, Any],
                           real_host_name: str = None, real_client_name: str = None) -> str:
    """Build filename like: 260308_214449_Bat(remilia)-蕾咪瞎玩(remilia).rep
    Character info comes from match_data (parsed from actual GAME_MATCH packet).
    Uses real names from INIT_SUCCESS packet if available, otherwise falls back to API names.
    """
    # Prefer real names from packet, fallback to API names
    host_name = _sanitize_name(real_host_name or game_info.get("host_name", "host"), "host")
    client_name = _sanitize_name(real_client_name or game_info.get("client_name", "client"), "client")
    timestamp = datetime.now().strftime("%y%m%d_%H%M%S")

    # Prefer character from match_data (actual value from GAME_MATCH packet)
    if replay.match_data:
        h_char = CHARACTERS.get(replay.match_data.host.character_id, "unk")
        c_char = CHARACTERS.get(replay.match_data.client.character_id, "unk")
    else:
        # Fallback to API game_info (string values like "remilia")
        h_char = game_info.get("host_character", "unk")
        c_char = game_info.get("client_character", "unk")
        if not h_char:
            h_char = "unk"
        if not c_char:
            c_char = "unk"

    return f"{timestamp}_{host_name}({h_char})-{client_name}({c_char}).rep"


def process_single_game(game_info: Dict, output_dir: str, duration: float) -> str:
    """Process a single game: connect, capture all matches until disconnect, save each match."""
    ip_port = game_info.get("ip", "")
    if ":" not in ip_port:
        log_error("Invalid IP:port format", ValueError(f"Missing colon in IP:port: {ip_port}"), {"ip_port": ip_port})
        return "error"

    host, port_str = ip_port.rsplit(":", 1)
    try:
        port = int(port_str)
    except ValueError as e:
        log_error("Invalid port number", e, {"port_str": port_str, "ip_port": ip_port})
        return "error"

    # Generate game ID for logging purposes
    game_id = f"{game_info.get('host_name', '')}_{game_info.get('client_name', '')}_{ip_port}"

    logger.info(f"[{ip_port}] Connecting...")
    # Register active game with detailed info for display
    h_char_api = game_info.get("host_character", "")
    c_char_api = game_info.get("client_character", "")
    stats.set_game_info(ip_port, ActiveGameInfo(
        ip=ip_port,
        host_name=game_info.get("host_name", "?"),
        client_name=game_info.get("client_name", "?"),
        host_char=h_char_api if h_char_api else "",
        client_char=c_char_api if c_char_api else "",
        status="connecting...",
    ))
    client = SpectatorClient(host, port, game_info)

    logger.info(f"[{ip_port}] Attempting handshake...")
    if not client.connect():
        logger.warning(f"[{ip_port}] Failed to connect")
        client.close()
        stats.update(errors=stats.errors + 1)
        stats.remove_game(ip_port)
        return "error"

    # Get real player names from INIT_SUCCESS packet
    real_host_name = None
    real_client_name = None
    if client.init_success_info:
        real_host_name = client.init_success_info.get("host_name")
        real_client_name = client.init_success_info.get("client_name")
        if real_host_name or real_client_name:
            logger.info(f"[{ip_port}] Using real names from packet: host='{real_host_name}', client='{real_client_name}'")
            # Update stats display with real names
            stats.set_game_info(ip_port, ActiveGameInfo(
                ip=ip_port,
                host_name=real_host_name or game_info.get("host_name", "?"),
                client_name=real_client_name or game_info.get("client_name", "?"),
                host_char=h_char_api if h_char_api else "",
                client_char=c_char_api if c_char_api else "",
                status="connected",
            ))

    # Build session directory for this game session (using real names if available)
    session_dir = _build_session_dir(game_info, output_dir, real_host_name, real_client_name)

    capture_duration = None if duration <= 0 else duration
    # Count existing replays in session directory for TODAY only
    # to avoid counting files from previous days when same players reconnect
    # Only count if directory already exists (i.e., reconnecting to same session)
    saved_count = 0
    if os.path.exists(session_dir):
        today_str = datetime.now().strftime("%y%m%d")
        existing_replays = glob.glob(os.path.join(session_dir, f"{today_str}_*.rep"))
        saved_count = len(existing_replays)
    # Create directory after checking existence
    os.makedirs(session_dir, exist_ok=True)
    # Also update the stats display with the initial count
    stats.update_game_saved_count(ip_port, saved_count)

    try:
        while True:
            logger.info(f"[{ip_port}] Capturing match (timeout={duration}s)...")
            stats.set_game_status(ip_port, "capturing...")

            # Reset replay_data for new match capture
            client.replay_data = ReplayData()

            replay = client.capture_replay(
                duration=capture_duration,
                stop_on_match_end=True,
                stop_immediately_on_match_end=False,
                stop_on_match_switch=True,
                stop_on_match_quiet=False,
                match_quiet_timeout=2.5,
                idle_timeout_after_end=5.0,
                idle_timeout_no_data=30.0,
                stop_on_no_progress=True,
                no_progress_timeout=15.0,
                stop_when_never_readable=True,
                never_readable_timeout=20.0,
                stop_on_empty_replay_marker=False,
            )

            # Save replay if we got frames
            if replay.frames:
                filename = _build_replay_filename(replay, game_info, real_host_name, real_client_name)
                filepath = os.path.join(session_dir, filename)
                if save_replay_simple(replay, filepath):
                    saved_count += 1
                    stats.update(recorded=stats.recorded + 1)
                    stats.update_game_saved_count(ip_port, saved_count)
                    logger.info(f"[{ip_port}] Saved match #{saved_count}: {filepath} ({len(replay.frames)} frames)")
                else:
                    logger.error(f"[{ip_port}] Failed to save replay to {filepath}")
                    stats.update(errors=stats.errors + 1)
            else:
                logger.warning(f"[{ip_port}] Match capture returned 0 frames (reason={replay.debug_stop_reason})")

            # Continue looping only on match_switch — the session is still alive
            if replay.debug_stop_reason == "match_switch":
                logger.info(f"[{ip_port}] Match ended, waiting for next match...")
                stats.set_game_status(ip_port, "waiting next match...")
                continue

            # Any other stop reason means the session is over
            logger.info(f"[{ip_port}] Session ended: reason={replay.debug_stop_reason}")
            break

    except Exception as e:
        log_error(f"Unexpected error during capture for {ip_port}", e, {
            "game_info": game_info,
            "session_dir": session_dir,
            "saved_count": saved_count
        })
        stats.update(errors=stats.errors + 1)
    finally:
        client.close()
        stats.remove_game(ip_port)

    if saved_count > 0:
        return "recorded"
    else:
        stats.update(errors=stats.errors + 1)
        return "error"


class ReplayService:
    """Replay recording service with continuous polling and dynamic game discovery"""

    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR, poll_interval: float = 30.0,
                 capture_duration: float = 120.0, max_workers: int = 30):
        self.output_dir = output_dir
        self.poll_interval = poll_interval
        self.capture_duration = capture_duration
        self.max_workers = max_workers
        self.running = False
        self.active_futures = {}  # game_id -> future
        self.executor = None
        self.lock = threading.Lock()

        # Enable debug logging if needed
        if os.environ.get("REPLAY_DEBUG"):
            logging.getLogger().setLevel(logging.DEBUG)

    def get_spectatable_games(self) -> List[Dict[str, Any]]:
        """Get list of spectatable games"""
        games = fetch_games()
        spectatable = [g for g in games if g.get("spectatable") and g.get("started")]
        logger.debug(f"Found {len(spectatable)} spectatable games out of {len(games)} total")
        return spectatable

    def _game_capture_callback(self, game_id: str, future):
        """Callback when a game capture task completes"""
        try:
            result = future.result()
            logger.info(f"Game completed: {game_id} -> {result}")
        except Exception as e:
            log_error(f"Error processing game {game_id}", e, {"game_id": game_id})
            stats.update(errors=stats.errors + 1)
        finally:
            with self.lock:
                if game_id in self.active_futures:
                    del self.active_futures[game_id]

    def poll_and_submit_new_games(self):
        """Fetch games and submit new ones without waiting"""
        logger.info("Fetching games from server...")
        games = self.get_spectatable_games()
        if not games:
            logger.debug("No spectatable games found")
            return 0

        stats.update(total=stats.total + len(games))

        submitted_count = 0
        for game in games:
            game_id = f"{game.get('host_name', '')}_{game.get('client_name', '')}_{game.get('ip', '')}"

            with self.lock:
                # Skip if already being processed
                if game_id in self.active_futures:
                    logger.debug(f"Game already being processed: {game_id}")
                    continue

                # Check if executor has capacity
                if len(self.active_futures) >= self.max_workers:
                    logger.debug(f"Max workers reached ({self.max_workers}), skipping: {game_id}")
                    continue

                logger.info(f"Submitting game for capture: {game_id}")
                future = self.executor.submit(
                    process_single_game,
                    game,
                    self.output_dir,
                    self.capture_duration
                )
                future.add_done_callback(lambda f, gid=game_id: self._game_capture_callback(gid, f))
                self.active_futures[game_id] = future
                submitted_count += 1

        logger.info(f"Submitted {submitted_count} new games, currently processing {len(self.active_futures)} total")
        return submitted_count

    def run_once(self):
        """Run one capture cycle (legacy mode, waits for all to complete)"""
        self.poll_and_submit_new_games()
        # Wait for all active games to complete
        while self.active_futures:
            time.sleep(1.0)
            print_status_display()

    def run(self):
        """Main service loop with continuous polling"""
        self.running = True

        os.makedirs(self.output_dir, exist_ok=True)

        print(f"=== Replay Service Started ===")
        print(f"Output: {self.output_dir} | Poll: {self.poll_interval}s | Duration: {self.capture_duration}s | Workers: {self.max_workers} (2 cores x 15)")
        print(f"Press Ctrl+C to stop")
        print("")

        # Create long-running executor
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            self.executor = executor

            # Start display update thread
            display_running = threading.Event()
            display_running.set()

            def _update_display():
                while display_running.is_set() and self.running:
                    print_status_display()
                    time.sleep(1.0)

            display_thread = threading.Thread(target=_update_display, daemon=True)
            display_thread.start()

            cycle_count = 0
            last_poll_time = 0

            while self.running:
                try:
                    current_time = time.time()

                    # Poll for new games every poll_interval seconds
                    if current_time - last_poll_time >= self.poll_interval:
                        cycle_count += 1
                        logger.info(f"=== Starting poll cycle #{cycle_count} ===")
                        self.poll_and_submit_new_games()
                        last_poll_time = current_time

                    # Small sleep to prevent busy loop
                    time.sleep(0.5)

                except KeyboardInterrupt:
                    logger.info("KeyboardInterrupt received, stopping...")
                    break
                except Exception as e:
                    log_error("Error in main service loop", e, {"cycle_count": cycle_count})
                    time.sleep(1)

            # Cleanup
            display_running.clear()
            display_thread.join(timeout=2.0)
            self.running = False

        print(f"\n=== Service Stopped ===")
        print_status_display()
        print("")

    def stop(self):
        """Stop the service"""
        self.running = False


def main():
    parser = argparse.ArgumentParser(description="Touhou 12.3 Replay Recording Service (Parallel)")
    parser.add_argument("--output", "-o", default=DEFAULT_OUTPUT_DIR, help="Output directory for replays")
    parser.add_argument("--poll", "-p", type=float, default=30.0, help="Poll interval in seconds")
    parser.add_argument("--duration", "-d", type=float, default=0.0,
                        help="Capture duration in seconds per game. Use 0 to wait until match end (default)")
    parser.add_argument("--workers", "-w", type=int, default=30, help="Max concurrent connections (default: 30 = 2 cores x 15 workers)")
    parser.add_argument("--once", "-1", action="store_true", help="Run once instead of continuously")

    args = parser.parse_args()

    service = ReplayService(
        output_dir=args.output,
        poll_interval=args.poll,
        capture_duration=args.duration,
        max_workers=args.workers
    )

    def signal_handler(sig, frame):
        service.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if args.once:
        service.run_once()
        print_status_display()
        print("")
    else:
        service.run()


if __name__ == "__main__":
    main()
