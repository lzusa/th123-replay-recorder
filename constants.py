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
