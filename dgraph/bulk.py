from prairiedog import recommended_procs

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

