import binascii
import os
import socket
import time
from datetime import datetime
from typing import Dict, List, Optional, Set

from constants import (
    CHARACTERS,
    GAME_LOADED,
    GAME_LOADED_ACK,
    GAME_MATCH,
    GAME_REPLAY,
    PACKET_HOST_GAME,
    PACKET_INIT_ERROR,
    PACKET_INIT_SUCCESS,
    PACKET_OLLEH,
    PACKET_QUIT,
    SPECTATE_GAME_ID_CANDIDATES,
)
from logging_config import log_error, logger
from models import FrameInput, ReplayData, normalize_frame_inputs, stats
from protocol import TouhouProtocol


class SpectatorClient:
    """Spectator client for capturing replays"""

    def __init__(self, host: str, port: int, game_info: Dict):
        self.host = host
        self.port = port
        self.game_info = game_info
        self.sock: Optional[socket.socket] = None
        self.replay_data = ReplayData()
        self.init_success_info: Optional[Dict[str, object]] = None
        self.selected_game_id = SPECTATE_GAME_ID_CANDIDATES[-1]
        self.running = False
        self.filepath = ""
        self.ip_port = f"{host}:{port}"

    def connect(self) -> bool:
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.settimeout(10.0)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            local_addr = ("0.0.0.0", 0)
            self.sock.bind(local_addr)
            local_port = self.sock.getsockname()[1]
            logger.info(f"[{self.ip_port}] Local socket bound to port {local_port}")

            hello_packet = TouhouProtocol.create_hello(self.host, self.port)
            logger.info(f"[{self.ip_port}] Sending HELLO ({len(hello_packet)} bytes)...")
            for i in range(5):
                self.sock.sendto(hello_packet, (self.host, self.port))
                logger.info(f"[{self.ip_port}] HELLO sent (attempt {i + 1}/5)")
                time.sleep(0.1)

            got_olleh = False
            logger.info(f"[{self.ip_port}] Waiting for OLLEH...")
            for _ in range(30):
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
                        data, _ = self.sock.recvfrom(4096)
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

            logger.info(f"[{self.ip_port}] Handshake complete, switching to connected mode")
            self.sock.connect((self.host, self.port))
            self.sock.settimeout(2.0)
            return True

        except Exception as e:
            log_error(
                f"Connection to {self.ip_port}",
                e,
                {
                    "host": self.host,
                    "port": self.port,
                    "game_info": self.game_info,
                },
            )
            return False

    def _receive_packet(self, timeout: float = 0.05) -> Optional[bytes]:
        try:
            self.sock.settimeout(timeout)
            return self.sock.recv(4096)
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
        miss_multiplier: int = 3,
    ) -> ReplayData:
        self.running = True
        start_time = time.time()
        current_match_id = 0
        frame_inputs_by_id: Dict[int, FrameInput] = {}
        raw_words_seen: Set[int] = set()
        requested_frame = 0xFFFFFFFF
        sent_initial_probe = False
        contiguous_word_frontier = 0
        last_sent_request_frame = 0xFFFFFFFF
        current_match_seen_at: Optional[float] = None
        initial_probe_send_count = 0
        last_status_update = 0.0
        last_request_time = 0.0
        last_packet_time = start_time
        match_end_detected_at: Optional[float] = None
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
        miss_burst = max(1, int(miss_multiplier or 1))

        def _missing_frames_upto(frame_limit: int) -> List[int]:
            if frame_limit <= 0:
                return []
            return [frame_id for frame_id in range(1, frame_limit + 1) if frame_id not in frame_inputs_by_id]

        def _max_captured_frame() -> int:
            if frame_inputs_by_id:
                return max(frame_inputs_by_id.keys())
            if not self.replay_data.frames:
                return -1
            return max(frame.frame_id for frame in self.replay_data.frames)

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

        def _choose_request_frame(now: float) -> int:
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

        try:
            while self.running and _has_time_left(time.time()):
                current_time = time.time()

                if idle_timeout_no_data is not None and (current_time - last_packet_time) > idle_timeout_no_data:
                    logger.info(f"[{self.ip_port}] Stopping due to no packets for {idle_timeout_no_data:.1f}s")
                    stop_reason = "idle_timeout_no_data"
                    break

                if current_time - last_request_time > request_interval:
                    send_request_frame = _choose_request_frame(current_time)
                    replay_req = TouhouProtocol.create_spectate_replay_request(current_match_id, send_request_frame)
                    try:
                        # 发送原有的周期性请求（保持主进度不变）
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

                    # 额外发送定向缺帧请求（不改变主请求节奏）——只发送最早一个缺帧的请求
                    try:
                        current_max_frame = _max_captured_frame()
                        missing = _missing_frames_upto(current_max_frame) if current_max_frame > 0 else []
                    except Exception:
                        missing = []

                    if missing:
                        # 回退：只对最早的一个缺帧目标突发请求（每帧重复 miss_burst 次），不改变主请求节奏
                        target_frame = missing[0]
                        try:
                            target_raw = target_frame * 2
                        except Exception:
                            target_raw = None
                        if target_raw is not None and target_raw != send_request_frame:
                            for attempt in range(miss_burst):
                                try:
                                    miss_req = TouhouProtocol.create_spectate_replay_request(current_match_id, target_raw)
                                    self.sock.send(miss_req)
                                    _dump_packet("SEND-MISS", miss_req)
                                    logger.debug(
                                        f"[{self.ip_port}] SEND-MISS missing_frame={target_frame} raw_word={target_raw} match_id={current_match_id} attempt={attempt+1}/{miss_burst} raw={miss_req.hex()}"
                                    )
                                except Exception:
                                    # 发包失败就忽略；下一次主循环会继续重试
                                    pass

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
                            logger.debug(f"[{self.ip_port}] RECV GAME_MATCH len={len(game_data)} raw={game_data.hex()}")
                            match_data = TouhouProtocol.parse_game_match(game_data)
                            if match_data:
                                self.replay_data.debug_session_signals += 1
                                self.replay_data.debug_match_packets += 1
                                has_readable_session_signal = True
                                has_seen_match_packet = True
                                last_match_packet_time = current_time
                                is_new_match = active_match_id is None or match_data.match_id != active_match_id

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
                                    self.replay_data.ended = True
                                    stop_reason = "match_switch"
                                    break

                                self.replay_data.match_data = match_data
                                current_match_id = match_data.match_id
                                if is_new_match:
                                    active_match_id = current_match_id
                                    seen_replay_for_active_match = False
                                    current_match_seen_at = current_time
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
                            else:
                                self.replay_data.debug_match_parse_failures += 1
                                logger.debug(
                                    f"[{self.ip_port}] Failed to parse GAME_MATCH payload len={len(game_data)} raw={game_data.hex()}"
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
                                            f"[{self.ip_port}] Replay gap before backfill: missing_words={max_replay_word_seen + 1}-{raw_first_word - 1} "
                                            f"incoming_words={raw_first_word}-{raw_frame_id}"
                                        )
                                    if max_replay_word_seen > 0 and raw_frame_id < max_replay_word_seen:
                                        logger.debug(
                                            f"[{self.ip_port}] Replay out-of-order/backfill packet: incoming_words={raw_first_word}-{raw_frame_id} "
                                            f"after_seen={max_replay_word_seen}"
                                        )
                                    if raw_frame_id > max_replay_word_seen:
                                        max_replay_word_seen = raw_frame_id

                                    for raw_word in range(raw_first_word, raw_frame_id + 1):
                                        raw_words_seen.add(raw_word)

                                    while (contiguous_word_frontier + 1) in raw_words_seen:
                                        contiguous_word_frontier += 1

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
                                empty_replay_streak = empty_replay_streak + 1 if is_empty_replay_marker else 0

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

                                replay_match_id = replay_info.get("match_id", 0)
                                if replay_match_id != current_match_id:
                                    logger.debug(f"Match ID changed: {current_match_id} -> {replay_match_id}")
                                    current_match_id = replay_match_id
                                if active_match_id is None:
                                    active_match_id = current_match_id
                                if current_match_id == active_match_id:
                                    seen_replay_for_active_match = True

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
                                    elif previous.client_input != incoming.client_input or previous.host_input != incoming.host_input:
                                        frame_inputs_by_id[frame_id] = incoming

                                if made_progress:
                                    last_progress_time = current_time
                                    has_real_replay_progress = True

                                self.replay_data.frames = list(frame_inputs_by_id.values())

                                if replay_info["frame_id"] > 0:
                                    last_progress_time = current_time
                                    has_real_replay_progress = True
                                    if replay_info["frame_id"] > last_replay_frame_seen:
                                        last_replay_frame_seen = replay_info["frame_id"]
                                    if replay_info.get("inputs") and replay_info["frame_id"] > last_replay_data_frame_seen:
                                        last_replay_data_frame_seen = replay_info["frame_id"]

                                current_max_frame = _max_captured_frame()
                                if current_max_frame > 0 and (
                                    current_time - last_missing_report_time >= 1.0
                                    or current_max_frame - last_missing_report_max_frame >= 200
                                ):
                                    _missing_frames_upto(current_max_frame)
                                    last_missing_report_time = current_time
                                    last_missing_report_max_frame = current_max_frame

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

                                    if replay_info.get("game_inputs_count", 0) == 0 and not replay_info.get("inputs"):
                                        empty_end_marker_seen_at = current_time
                                        empty_end_marker_max_word = max(empty_end_marker_max_word, raw_announced_end_word)
                                        logger.debug(
                                            f"[{self.ip_port}] Empty end marker received: raw_frame={raw_frame_id}, raw_end_frame={raw_end_frame_id}, "
                                            f"frontier={contiguous_word_frontier}, last_data_frame={last_replay_data_frame_seen}"
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
                            try:
                                ack = bytes([0x0E, GAME_LOADED_ACK, scene_id])
                                self.sock.send(ack)
                            except Exception:
                                pass

                if stop_reason:
                    break

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

                if current_time - last_status_update > 1.0:
                    elapsed = int(current_time - start_time)
                    frames = len(self.replay_data.frames)
                    status_text = f"{elapsed}s, {frames}f" if duration is None else f"{elapsed}s/{int(duration)}s, {frames}f"
                    stats.set_game_status(self.ip_port, status_text)
                    host_char = ""
                    client_char = ""
                    if self.replay_data.match_data:
                        host_char = CHARACTERS.get(self.replay_data.match_data.host.character_id, "unk")
                        client_char = CHARACTERS.get(self.replay_data.match_data.client.character_id, "unk")
                    stats.update_game_capture(
                        self.ip_port,
                        current_match_id,
                        frames,
                        host_char=host_char,
                        client_char=client_char,
                    )
                    last_status_update = current_time

                time.sleep(0.016)

        except Exception as e:
            if not stop_reason:
                stop_reason = "capture_exception"
            log_error(
                f"Unexpected capture error for {self.ip_port}",
                e,
                {
                    "current_match_id": current_match_id,
                    "requested_frame": requested_frame,
                    "last_replay_frame_seen": last_replay_frame_seen,
                    "captured_frames": len(frame_inputs_by_id),
                },
            )

        finally:
            self.replay_data.frames = list(frame_inputs_by_id.values())
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
                        f"last_replay_frame={last_replay_frame_seen} requested_frame={requested_frame} match_id={current_match_id}\n"
                    )
                    dump_fh.close()
                except Exception:
                    pass

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
            f"session_packets={self.replay_data.debug_session_signals}, match_packets={self.replay_data.debug_match_packets}, "
            f"replay_packets={self.replay_data.debug_replay_packets}, match_parse_failures={self.replay_data.debug_match_parse_failures}, "
            f"replay_parse_failures={self.replay_data.debug_replay_parse_failures}, last_replay_frame={last_replay_frame_seen}, "
            f"requested_frame={requested_frame}, match_id={current_match_id}"
        )
        _missing_frames_upto(_max_captured_frame())
        return self.replay_data

    def close(self):
        if self.sock:
            try:
                self.sock.send(bytes([PACKET_QUIT]))
            except Exception:
                pass
            self.sock.close()
            self.sock = None