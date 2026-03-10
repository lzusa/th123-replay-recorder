import struct
import zlib
from typing import Optional, Dict, Any

from constants import (
    PACKET_HELLO, PACKET_PUNCH, PACKET_OLLEH, PACKET_CHAIN,
    PACKET_INIT_REQUEST, PACKET_INIT_SUCCESS, PACKET_CLIENT_GAME,
    DEFAULT_GAME_ID, STUFF_BYTES,
    GAME_LOADED, GAME_MATCH, GAME_REPLAY, GAME_REPLAY_REQUEST,
)
from models import PlayerData, MatchData
from logging_config import logger, log_init_success_name_debug


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
        base = bytes([PACKET_INIT_REQUEST]) + game_id + STUFF_BYTES + bytes([0x00])
        padding_len = 65 - len(base)
        return base + bytes(padding_len)

    @staticmethod
    def create_init_request_response(response_data: bytes) -> bytes:
        """Create response to INIT_SUCCESS for spectating"""
        if len(response_data) < 10:
            return None
        spectate_info = response_data[5:9]
        return bytes([PACKET_INIT_SUCCESS]) + response_data[1:5] + spectate_info

    @staticmethod
    def parse_init_success(data: bytes) -> Optional[Dict[str, Any]]:
        """Parse INIT_SUCCESS payload."""
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

        if len(data_blob) >= 68:
            host_raw = data_blob[0:32]
            client_raw = data_blob[32:64]
            swr_disabled = struct.unpack("<I", data_blob[64:68])[0]

            def _extract_profile_name_bytes(padded: bytes) -> bytes:
                if len(padded) < 2:
                    return b""
                body = padded[1:]
                nul_pos = body.find(b"\x00")
                if nul_pos >= 0:
                    return body[:nul_pos]
                return body

            host_bytes = _extract_profile_name_bytes(host_raw)
            client_bytes = _extract_profile_name_bytes(client_raw)

            def _decode_name(name_bytes: bytes) -> str:
                if not name_bytes:
                    return ""
                try:
                    name_gbk = name_bytes.decode("gbk", errors="strict")
                    cjk_chars = sum(1 for c in name_gbk if '\u4e00' <= c <= '\u9fff' or '\u3000' <= c <= '\u303f')
                    if cjk_chars > 0:
                        return name_gbk
                except UnicodeDecodeError:
                    pass
                try:
                    name_sjis = name_bytes.decode("shift_jis", errors="strict")
                    valid_sjis = sum(1 for c in name_sjis
                                     if ord(c) < 128 or
                                        '\u3040' <= c <= '\u309f' or
                                        '\u30a0' <= c <= '\u30ff' or
                                        '\u4e00' <= c <= '\u9fff')
                    if valid_sjis == len(name_sjis):
                        return name_sjis
                except UnicodeDecodeError:
                    pass
                try:
                    return name_bytes.decode("gbk", errors="replace")
                except:
                    return name_bytes.decode("shift_jis", errors="ignore")

            host_name = _decode_name(host_bytes)
            client_name = _decode_name(client_bytes)

            log_init_success_name_debug({
                "event": "parse_init_success_names",
                "spectate_info": result["spectate_info"],
                "data_size": result["data_size"],
                "raw_len": result["raw_len"],
                "host_raw_hex": host_raw.hex(),
                "client_raw_hex": client_raw.hex(),
                "host_byte0": f"0x{host_raw[0]:02x}",
                "client_byte0": f"0x{client_raw[0]:02x}",
                "host_name_bytes_hex": host_bytes.hex(),
                "client_name_bytes_hex": client_bytes.hex(),
                "host_name": host_name,
                "client_name": client_name,
                "swr_disabled": swr_disabled,
            })

            result["host_name"] = host_name
            result["client_name"] = client_name
            result["swr_disabled"] = swr_disabled
        else:
            log_init_success_name_debug({
                "event": "parse_init_success_short_data_blob",
                "data_size": result.get("data_size", 0),
                "raw_len": result.get("raw_len", 0),
                "data_blob_len": len(data_blob),
                "data_blob_hex": data_blob[:96].hex(),
            })

        return result

    @staticmethod
    def create_spectate_replay_request(match_id: int, frame_id: int) -> bytes:
        """Create spectate replay request"""
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
        def _parse_player_match_data(buf: bytes, start: int) -> Optional[tuple]:
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
        if len(data) < 2:
            return None
        try:
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
            if len(decompressed) < 10:
                logger.debug(f"Replay payload too short: {len(decompressed)}")
                return None
            result = {}
            offset = 0

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

            result["inputs"] = []
            for i in range(replay_inputs_count):
                if offset + 4 > len(decompressed):
                    logger.debug(f"Truncated input data at input {i}")
                    break
                client_input = struct.unpack("<H", decompressed[offset:offset + 2])[0]
                host_input = struct.unpack("<H", decompressed[offset + 2:offset + 4])[0]
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
