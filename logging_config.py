import os
import json
import logging
import traceback
from typing import Dict, Any

# Configure logging - output to file only to keep console clean for status display
log_level = logging.DEBUG if os.environ.get("REPLAY_DEBUG") else logging.INFO

# Ensure logs directory exists
LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOGS_DIR, 'replay_service.log')
ERROR_LOG_FILE = os.path.join(LOGS_DIR, 'error_details.log')
INIT_SUCCESS_NAME_DEBUG_FILE = os.path.join(LOGS_DIR, 'init_success_name_debug.log')

logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
    ]
)
logger = logging.getLogger('replay_recorder')

# Error logger for detailed error tracking
error_logger = logging.getLogger('error_details')
error_logger.setLevel(logging.ERROR)
error_handler = logging.FileHandler(ERROR_LOG_FILE, encoding='utf-8')
error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
error_logger.addHandler(error_handler)

# Always-on debug logger for INIT_SUCCESS profile name parsing.
init_success_name_logger = logging.getLogger('init_success_name_debug')
init_success_name_logger.setLevel(logging.INFO)
init_success_name_logger.propagate = False
if not init_success_name_logger.handlers:
    init_success_name_handler = logging.FileHandler(INIT_SUCCESS_NAME_DEBUG_FILE, encoding='utf-8')
    init_success_name_handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    init_success_name_logger.addHandler(init_success_name_handler)


def log_init_success_name_debug(record: Dict[str, Any]):
    """Write INIT_SUCCESS name parsing diagnostics to a dedicated file."""
    try:
        init_success_name_logger.info(json.dumps(record, ensure_ascii=False))
    except Exception:
        pass


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
