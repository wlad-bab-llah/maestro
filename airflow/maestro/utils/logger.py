"""MAESTRO Centralized Logging"""

import logging
import sys
from typing import Optional

class MaestroFormatter(logging.Formatter):
    """Custom formatter for MAESTRO logs"""
   
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green  
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m'      # Reset
    }
   
    def format(self, record):
        if hasattr(sys.stderr, 'isatty') and sys.stderr.isatty():
            color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
            reset = self.COLORS['RESET']
            log_format = f"{color}[%(asctime)s] [MAESTRO] [%(levelname)s] [%(name)s] %(message)s{reset}"
        else:
            log_format = "[%(asctime)s] [MAESTRO] [%(levelname)s] [%(name)s] %(message)s"
       
        formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')
        return formatter.format(record)

def get_maestro_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """Get configured MAESTRO logger"""
    logger = logging.getLogger(f"maestro.{name}")
    log_level = getattr(logging, (level or 'INFO').upper(), logging.INFO)
    logger.setLevel(log_level)
   
    if not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(MaestroFormatter())
        logger.addHandler(console_handler)
        logger.propagate = False
   
    return logger
