import subprocess
import logging
import glob
import shutil
import pathlib

from prairiedog import recommended_procs

log = logging.getLogger('prairiedog')

# Calculations for Dgraph Bulk processes, this was based off our tests
GB_PER_PROC_DGRAPH = 94 / 12

recommended_procs_dgraph = recommended_procs(GB_PER_PROC_DGRAPH)


def dgraph_bulk_cmd(
    rdfs: str = 'outputs/samples/', schema: str = 'dgraph/kmers.schema',
        zero_port: int = 5080) -> str:
    run_cmd = "dgraph bulk \
    -r {rdfs} \
    -s {schema} \
    -j {n_procs} \
    --map_shards=1 \
    --reduce_shards=1 \
    --http localhost:8001 \
    --zero=localhost:{zero_port}".format(
        rdfs=rdfs, schema=schema, n_procs=recommended_procs_dgraph,
        zero_port=zero_port)
    return run_cmd


def run_dgraph_bulk(cwd: str = '.', move_to: str = None, **kwargs):
    cmd = dgraph_bulk_cmd(**kwargs)
    log.info("Executing {} from {}".format(cmd, cwd))
    subprocess.run(cmd, shell=True, cwd=cwd)
    log.info("Done running dgraph bulk")
    p = pathlib.Path(cwd, 'out', '0', 'p')
    if not p.exists():
        raise Exception("Path {} was not found".format(p))
    else:
        files = glob.glob("{}/**".format(p), recursive=True)
        log.info("Output in {} is {}".format(p, files))

    if move_to is not None:
        log.info("Will move files to {}".format(move_to))
        shutil.copytree(cwd, move_to)
