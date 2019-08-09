# -*- coding: utf-8 -*-

"""Top-level package for prairiedog."""

__author__ = """Kevin Le"""
__email__ = 'kevin.kent.le@gmail.com'
__version__ = '0.1.1'

import psutil

from prairiedog.logger import setup_logging

# Initialize logging
setup_logging()


def recommended_procs(gb_per_proc) -> int:
    vm_bytes = psutil.virtual_memory().total
    vm_gb = vm_bytes / (1 << 30)
    procs_by_mem = int(vm_gb / gb_per_proc)
    cpus = psutil.cpu_count()
    if procs_by_mem < cpus:
        # Memory is the limitation
        return procs_by_mem
    else:
        # Plenty of memory, use all CPUs
        return cpus
