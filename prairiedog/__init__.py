# -*- coding: utf-8 -*-

"""Top-level package for prairiedog."""

__author__ = """Kevin Le"""
__email__ = 'kevin.kent.le@gmail.com'
__version__ = '0.1.1'

# Initialize logging
import coloredlogs
import logging


log = logging.getLogger('prairiedog')
coloredlogs.install(level='DEBUG')

fmt = '%(asctime)s %(name)s[%(process)d] %(levelname)s %(message)s'
formatter = coloredlogs.ColoredFormatter(fmt)
fh = logging.FileHandler('prairiedog.log')
fh.setFormatter(formatter)
log.addHandler(fh)
