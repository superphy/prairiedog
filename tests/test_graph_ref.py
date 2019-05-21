import pandas as pd
from prairiedog.kmer_graph import KmerGraph
from prairiedog import config

def test_pandas_read_mic_csv():
    pd.read_csv('tests/public_mic_class_dataframe_test.csv')
    pd.read_csv('samples/public_mic_class_dataframe.csv')
    assert True


def test_graphref_output():
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

    ####
    #   KMERS_graph_indicator.txt
    ####
    with open(gr.graph_indicator) as f:
        lines = [int(li.rstrip()) for li in f.readlines()]
    # The number of lines written should equal the number of unique nodes per
    # file
    assert len(lines) == gr.n
    # The max graph indicator id should be the number of files
    assert max(lines) == len(kmg.km_list)
    # Min should not be 0
    assert min(lines) == 1

    ####
    #   KMERS_graph_labels.txt
    ####
    with open(gr._get_graph_label_file('AMP')) as f:
        lines = f.readlines()
    # The KMERS_graph_labels_AMP.txt file should only have as many lines as
    # there are genome files processed
    assert len(lines) == len(kmg.km_list)

    # Write out all maps and node_*.txt files
    kmg.gr.close()

    ####
    #   KMERS_node_labels.txt
    ####
    with open(gr.node_labels) as f:
        lines = [int(li.rstrip()) for li in f.readlines()]
    assert len(lines) == gr.n

    ####
    #   KMERS_node_attributes.txt
    ####
    with open(gr.node_attributes) as f:
        lines = [int(li.rstrip()) for li in f.readlines()]
    assert len(lines) == gr.n
    # The node attribute we use is the kmer, the max of which is 4^k
    assert max(lines) <= 4**config.K
    # Min should not be 0
    assert min(lines) == 1

    ####
    #   Check that we can read all dictionary mapping files
    ####
    pd.read_csv(gr.file_mapping)
    pd.read_csv(gr.mic_mapping)
    pd.read_csv(gr.kmer_mapping)
    pd.read_csv(gr.label_mapping)
