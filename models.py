import threading
from dataclasses import dataclass, field
from typing import Optional, List, Dict


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
