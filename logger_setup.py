# logger_setup.py
import logging
import sys
import coloredlogs

"""DEBUG → голубой
INFO → зелёный
WARNING → жёлтый
ERROR → красный
CRITICAL → фиолетовый
"""
def setup_logger(name="bot"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Кастомные цвета
    FIELD_STYLES = {
        'asctime': {'color': 'green'},
        'levelname': {'bold': True, 'color': 'white'},
        'name': {'color': 'blue'}
    }

    LEVEL_STYLES = {
        'debug': {'color': 'cyan'},
        'info': {'color': 'green'},
        'warning': {'color': 'yellow'},
        'error': {'color': 'red'},
        'critical': {'color': 'magenta', 'bold': True}
    }

    coloredlogs.install(
        level='INFO',
        logger=logger,
        stream=sys.stdout,
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level_styles=LEVEL_STYLES,
        field_styles=FIELD_STYLES
    )

    return logger
