import pandas as pd
from prairiedog.kmer_graph import KmerGraph
from prairiedog import config

def test_pandas_read_mic_csv():
    pd.read_csv('tests/public_mic_class_dataframe_test.csv')
    pd.read_csv('samples/public_mic_class_dataframe.csv')
    assert True


def test_graphref_incr_node():
    # Set the config MIC csv to use our test one
    config.MIC_CSV = 'tests/public_mic_class_dataframe_test.csv'
    # Use KmerGraph to drive graph creation for two files
    kmg = KmerGraph(
        [
            'tests/SRR1060582_SHORTENED.fasta',
            'tests/SRR1106609_SHORTENED.fasta'
        ]
    )
    # Alias a variable to the inner GraphRef
    gr = kmg.gr

    with open(gr.graph_indicator) as f:
        lines = f.readlines()
    # The number of lines written should equal the number of unique nodes per
    # file
    assert len(lines) == gr.n

    with open(gr._get_graph_label_file('AMP')) as f:
        lines = f.readlines()
    # The KMERS_graph_labels_AMP.txt file should only have as many lines as
    # there are genome files processed
    assert len(lines) == len(kmg.km_list)
