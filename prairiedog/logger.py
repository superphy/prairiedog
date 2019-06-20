# Initialize logging
import coloredlogs
import logging


def setup_logging(level: str = 'DEBUG'):
    log = logging.getLogger('prairiedog')
    coloredlogs.install(level=level)

    fmt = '%(asctime)s %(name)s[%(process)d] %(levelname)s %(message)s'
    formatter = coloredlogs.ColoredFormatter(fmt)
    fh = logging.FileHandler('prairiedog.log')
    fh.setFormatter(formatter)
    log.addHandler(fh)
