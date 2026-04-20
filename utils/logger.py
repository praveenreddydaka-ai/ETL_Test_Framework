import logging
import os
from datetime import datetime, timedelta

def cleanup_old_logs(log_dir, days=3):
    """
    Delete log files older than given days
    """
    now = datetime.now()

    for file in os.listdir(log_dir):
        file_path = os.path.join(log_dir, file)

        if os.path.isfile(file_path):
            file_time = datetime.fromtimestamp(os.path.getmtime(file_path))

            if now - file_time > timedelta(days=days):
                os.remove(file_path)

def setup_logger():

    # Create logs folder if not exists
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    cleanup_old_logs(log_dir, days=0)

    # Create unique log file per run
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"etl_log_{timestamp}.log")

    # Create logger
    logger = logging.getLogger("ETLFramework")
    logger.setLevel(logging.INFO)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    # Console handler (optional but useful)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Format
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Attach handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger