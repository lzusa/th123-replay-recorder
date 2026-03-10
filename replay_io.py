import glob
import json
import os
import struct
from datetime import datetime
from typing import Any, Dict, List, Optional

from constants import CHARACTERS, GAMES_URL
from logging_config import log_error, logger
from models import FrameInput, PlayerData, ReplayData, normalize_frame_inputs


def save_replay_simple(replay: ReplayData, filepath: str) -> bool:
    """Save replay in client-like .rep layout."""
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

            deck_count = min(len(deck_cards), 20)
            deck_cards = deck_cards[:20] + [0] * max(0, 20 - len(deck_cards))

            buf = bytearray()
            buf.append(char_id)
            buf.append(skin_id)
            buf.extend(struct.pack("<I", deck_count))
            for card in deck_cards:
                buf.extend(struct.pack("<H", card))
            buf.append(side_marker & 0xFF)
            buf.append(0x00)
            buf.append(disabled_sim & 0xFF)
            return bytes(buf)

        os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else ".", exist_ok=True)
        with open(filepath, "wb") as handle:
            header = bytearray()
            header.extend(bytes([0xD2, 0x00]))
            header.extend(bytes([0x00, 0x00, 0x00, 0x00]))
            header.extend(bytes([0x03, 0x09]))
            header.extend(bytes([0x06]))
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
            header.extend(bytes([0x03]))
            header.extend(bytes([stage_id, music_id]))
            header.extend(struct.pack("<I", random_seed))

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
                    first_input = int(frame.host_input) & 0xFFFF
                    second_input = int(frame.client_input) & 0xFFFF
                input_words.append(first_input)
                input_words.append(second_input)

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
            handle.write(header)
            for word in input_words:
                handle.write(struct.pack("<H", word))

        return True
    except Exception as e:
        log_error(
            f"Failed to save replay to {filepath}",
            e,
            {
                "filepath": filepath,
                "frames_count": len(replay.frames) if replay.frames else 0,
                "match_data": replay.match_data is not None,
            },
        )
        return False


def fetch_games() -> List[Dict[str, Any]]:
    """Fetch available games from server"""
    try:
        import urllib.request

        logger.info(f"Fetching games from {GAMES_URL}...")
        req = urllib.request.Request(GAMES_URL)
        req.add_header("User-Agent", "Mozilla/5.0")
        req.add_header("Cache-Control", "no-cache, no-store, must-revalidate")
        req.add_header("Pragma", "no-cache")
        req.add_header("Expires", "0")
        with urllib.request.urlopen(req, timeout=10) as response:
            data = response.read()
            games = json.loads(data)
            logger.info(f"Fetched {len(games)} total games from server")
            return games
    except Exception as e:
        log_error("Failed to fetch games from server", e, {"url": GAMES_URL})
        return []


def sanitize_name(name: str, fallback: str) -> str:
    safe = (name or fallback).replace("/", "_").replace("\\", "_").replace(":", "_")
    return safe[:20]


def build_session_dir(
    game_info: Dict[str, Any],
    output_dir: str,
    real_host_name: str = None,
    real_client_name: str = None,
) -> str:
    host_name = sanitize_name(real_host_name or game_info.get("host_name", "host"), "host")
    client_name = sanitize_name(real_client_name or game_info.get("client_name", "client"), "client")
    host_country = game_info.get("host_country", "")
    client_country = game_info.get("client_country", "")
    date_str = datetime.now().strftime("%y%m%d")
    dir_name = f"{date_str}_{host_name}({host_country})-{client_name}({client_country})"
    return os.path.join(output_dir, dir_name)


def build_replay_filename(
    replay: ReplayData,
    game_info: Dict[str, Any],
    real_host_name: str = None,
    real_client_name: str = None,
) -> str:
    host_name = sanitize_name(real_host_name or game_info.get("host_name", "host"), "host")
    client_name = sanitize_name(real_client_name or game_info.get("client_name", "client"), "client")
    timestamp = datetime.now().strftime("%y%m%d_%H%M%S")

    if replay.match_data:
        host_char = CHARACTERS.get(replay.match_data.host.character_id, "unk")
        client_char = CHARACTERS.get(replay.match_data.client.character_id, "unk")
    else:
        host_char = game_info.get("host_character", "unk") or "unk"
        client_char = game_info.get("client_character", "unk") or "unk"

    return f"{timestamp}_{host_name}({host_char})-{client_name}({client_char}).rep"


def count_today_replays(session_dir: str) -> int:
    if not os.path.exists(session_dir):
        return 0
    today_str = datetime.now().strftime("%y%m%d")
    existing_replays = glob.glob(os.path.join(session_dir, f"{today_str}_*.rep"))
    return len(existing_replays)