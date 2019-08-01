from pathlib import Path

import psutil

# Calculations for Dgraph Bulk processes, this was based off our tests
GB_PER_PROC = 94 / 12


def recommended_procs() -> int:
    vm_bytes = psutil.virtual_memory().total
    vm_gb = vm_bytes / (1 << 30)
    procs_by_mem = int(vm_gb / GB_PER_PROC)
    cpus = psutil.cpu_count()
    if procs_by_mem < cpus:
        # Memory is the limitation
        return procs_by_mem
    else:
        # Plenty of memory, use all CPUs
        return cpus


def dgraph_bulk_cmd(rdfs: Path, schema: Path, zero_port: int = 5080) -> str:
    run_cmd = "dgraph bulk \
    -f {rdfs} \
    -s {schema} \
    -j {n_procs} \
    --map_shards=1 \
    --reduce_shards=1 \
    --http localhost:8001 \
    --zero=localhost:{zero_port}".format(
        rdfs=rdfs, schema=schema, n_procs=recommended_procs(),
        zero_port=zero_port)
    return run_cmd

