from prairiedog.kmer_graph import KmerGraph


def test_graphref_incr_node():
    # Use KmerGraph to drive graph creation for two files
    kmg = KmerGraph(
        [
            'tests/SRR1060582.fasta',
            'tests/SRR1106609.fasta'
        ]
    )
    # Alias a variable to the inner GraphRef
    gr = kmg.gr
    with open(gr.graph_indicator) as f:
        lines = f.readlines()
    # The number of lines written should equal the number of unique nodes per
    # file
    assert len(lines) == gr.n
