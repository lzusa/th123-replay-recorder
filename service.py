import os
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

from constants import DEFAULT_OUTPUT_DIR
from display import print_status_display
from logging_config import log_error, logger
from models import ActiveGameInfo, ReplayData, stats
from replay_io import (
    build_replay_filename,
    build_session_dir,
    count_today_replays,
    fetch_games,
    save_replay_simple,
)
from spectator import SpectatorClient


def process_single_game(game_info: Dict[str, Any], output_dir: str, duration: float) -> str:
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

    logger.info(f"[{ip_port}] Connecting...")
    host_char_api = game_info.get("host_character", "")
    client_char_api = game_info.get("client_character", "")
    stats.set_game_info(
        ip_port,
        ActiveGameInfo(
            ip=ip_port,
            host_name=game_info.get("host_name", "?"),
            client_name=game_info.get("client_name", "?"),
            host_char=host_char_api if host_char_api else "",
            client_char=client_char_api if client_char_api else "",
            status="connecting...",
        ),
    )
    client = SpectatorClient(host, port, game_info)

    logger.info(f"[{ip_port}] Attempting handshake...")
    if not client.connect():
        logger.warning(f"[{ip_port}] Failed to connect")
        client.close()
        stats.update(errors=stats.errors + 1)
        stats.remove_game(ip_port)
        return "error"

    real_host_name = None
    real_client_name = None
    if client.init_success_info:
        real_host_name = client.init_success_info.get("host_name")
        real_client_name = client.init_success_info.get("client_name")
        if real_host_name or real_client_name:
            logger.info(f"[{ip_port}] Using real names from packet: host='{real_host_name}', client='{real_client_name}'")
            stats.set_game_info(
                ip_port,
                ActiveGameInfo(
                    ip=ip_port,
                    host_name=real_host_name or game_info.get("host_name", "?"),
                    client_name=real_client_name or game_info.get("client_name", "?"),
                    host_char=host_char_api if host_char_api else "",
                    client_char=client_char_api if client_char_api else "",
                    status="connected",
                ),
            )

    session_dir = build_session_dir(game_info, output_dir, real_host_name, real_client_name)
    capture_duration = None if duration <= 0 else duration
    saved_count = count_today_replays(session_dir)
    os.makedirs(session_dir, exist_ok=True)
    stats.update_game_saved_count(ip_port, saved_count)

    def persist_replay(replay: ReplayData, is_partial: bool = False) -> bool:
        nonlocal saved_count

        if not replay.frames:
            return False

        filename = build_replay_filename(replay, game_info, real_host_name, real_client_name)
        filepath = os.path.join(session_dir, filename)
        if save_replay_simple(replay, filepath):
            saved_count += 1
            stats.update(recorded=stats.recorded + 1)
            stats.update_game_saved_count(ip_port, saved_count)
            replay_kind = "partial replay" if is_partial else "replay"
            logger.info(
                f"[{ip_port}] Saved {replay_kind} #{saved_count}: {filepath} "
                f"({len(replay.frames)} frames, reason={replay.debug_stop_reason})"
            )
            return True

        logger.error(f"[{ip_port}] Failed to save replay to {filepath}")
        stats.update(errors=stats.errors + 1)
        return False

    try:
        replay_saved_for_current_capture = False
        while True:
            logger.info(f"[{ip_port}] Capturing match (timeout={duration}s)...")
            stats.set_game_status(ip_port, "capturing...")
            client.replay_data = ReplayData()
            replay_saved_for_current_capture = False

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

            if replay.frames:
                replay_saved_for_current_capture = persist_replay(replay)
            else:
                logger.warning(f"[{ip_port}] Match capture returned 0 frames (reason={replay.debug_stop_reason})")

            if replay.debug_stop_reason == "match_switch":
                logger.info(f"[{ip_port}] Match ended, waiting for next match...")
                stats.set_game_status(ip_port, "waiting next match...")
                continue

            logger.info(f"[{ip_port}] Session ended: reason={replay.debug_stop_reason}")
            break

    except Exception as e:
        if not replay_saved_for_current_capture and client.replay_data.frames:
            logger.info(
                f"[{ip_port}] Capture failed after receiving frames, attempting to save partial replay "
                f"(reason={client.replay_data.debug_stop_reason or 'exception'})"
            )
            replay_saved_for_current_capture = persist_replay(client.replay_data, is_partial=True)
        log_error(
            f"Unexpected error during capture for {ip_port}",
            e,
            {
                "game_info": game_info,
                "session_dir": session_dir,
                "saved_count": saved_count,
            },
        )
        stats.update(errors=stats.errors + 1)
    finally:
        client.close()
        stats.remove_game(ip_port)

    if saved_count > 0:
        return "recorded"
    stats.update(errors=stats.errors + 1)
    return "error"


class ReplayService:
    """Replay recording service with continuous polling and dynamic game discovery"""

    def __init__(
        self,
        output_dir: str = DEFAULT_OUTPUT_DIR,
        poll_interval: float = 30.0,
        capture_duration: float = 120.0,
        max_workers: int = 30,
        only_cn: bool = False,
    ):
        self.output_dir = output_dir
        self.poll_interval = poll_interval
        self.capture_duration = capture_duration
        self.max_workers = max_workers
        self.only_cn = bool(only_cn)
        self.running = False
        self.active_futures = {}
        self.executor = None
        self.lock = threading.Lock()

    def get_spectatable_games(self) -> List[Dict[str, Any]]:
        games = fetch_games()
        spectatable = [game for game in games if game.get("spectatable") and game.get("started")]
        if self.only_cn:
            filtered = []
            for game in spectatable:
                host_country = (game.get("host_country") or "").lower()
                client_country = (game.get("client_country") or "").lower()
                if host_country == "cn" and client_country == "cn":
                    filtered.append(game)
            logger.debug(f"Filtering spectatable games to only-cn: {len(filtered)} remain from {len(spectatable)}")
            spectatable = filtered
        logger.debug(f"Found {len(spectatable)} spectatable games out of {len(games)} total")
        return spectatable

    def _game_capture_callback(self, game_id: str, future):
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
                if game_id in self.active_futures:
                    logger.debug(f"Game already being processed: {game_id}")
                    continue
                if len(self.active_futures) >= self.max_workers:
                    logger.debug(f"Max workers reached ({self.max_workers}), skipping: {game_id}")
                    continue
                logger.info(f"Submitting game for capture: {game_id}")
                future = self.executor.submit(process_single_game, game, self.output_dir, self.capture_duration)
                future.add_done_callback(lambda future_obj, gid=game_id: self._game_capture_callback(gid, future_obj))
                self.active_futures[game_id] = future
                submitted_count += 1

        logger.info(f"Submitted {submitted_count} new games, currently processing {len(self.active_futures)} total")
        return submitted_count

    def run_once(self):
        self.poll_and_submit_new_games()
        while self.active_futures:
            time.sleep(1.0)
            print_status_display()

    def run(self):
        self.running = True
        os.makedirs(self.output_dir, exist_ok=True)

        print("=== Replay Service Started ===")
        print(
            f"Output: {self.output_dir} | Poll: {self.poll_interval}s | Duration: {self.capture_duration}s | "
            f"Workers: {self.max_workers} (2 cores x 15)"
        )
        print("Press Ctrl+C to stop")
        print("")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            self.executor = executor
            display_running = threading.Event()
            display_running.set()

            def _update_display():
                while display_running.is_set() and self.running:
                    print_status_display()
                    time.sleep(1.0)

            display_thread = threading.Thread(target=_update_display, daemon=True)
            display_thread.start()

            cycle_count = 0
            last_poll_time = 0.0
            while self.running:
                try:
                    current_time = time.time()
                    if current_time - last_poll_time >= self.poll_interval:
                        cycle_count += 1
                        logger.info(f"=== Starting poll cycle #{cycle_count} ===")
                        self.poll_and_submit_new_games()
                        last_poll_time = current_time
                    time.sleep(0.5)
                except KeyboardInterrupt:
                    logger.info("KeyboardInterrupt received, stopping...")
                    break
                except Exception as e:
                    log_error("Error in main service loop", e, {"cycle_count": cycle_count})
                    time.sleep(1)

            display_running.clear()
            display_thread.join(timeout=2.0)
            self.running = False

        print("\n=== Service Stopped ===")
        print_status_display()
        print("")

    def stop(self):
        self.running = False


def run_main(args):
    service = ReplayService(
        output_dir=args.output,
        poll_interval=args.poll,
        capture_duration=args.duration,
        max_workers=args.workers,
        only_cn=bool(getattr(args, "only_cn", False)),
    )

    def signal_handler(_sig, _frame):
        service.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if args.once:
        service.run_once()
        print_status_display()
        print("")
    else:
        service.run()