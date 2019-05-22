import subprocess
import pickle

import pandas as pd
import numpy as np


def test_pandas_read_mic_csv():
    pd.read_csv('tests/public_mic_class_dataframe_test.csv')
    pd.read_csv('samples/public_mic_class_dataframe.csv')
    assert True


def test_graphref_output(setup_snakefile):
    # Set the config MIC csv to use our test one
    mic_csv = 'tests/public_mic_class_dataframe_test.csv'

    # Use Snakemake to drive graph creation for two files
    subprocess.run('snakemake --config graph_labels={}'.format(mic_csv),
                   check=True, shell=True)

    # GraphRef created by snakemake
    gr = pickle.load(
        open('outputs/graphref_final.pkl', 'rb')
    )

    ####
    #   KMERS_graph_indicator.txt
    ####
    with open(gr.graph_indicator) as f:
        lines = [int(li.rstrip()) for li in f.readlines()]
    # The number of lines written should equal the number of unique nodes per
    # file
    # assert len(lines) == gr.n
    # The max graph indicator id should be the number of files
    assert max(lines) == len(setup_snakefile)

    ####
    #   KMERS_graph_labels.txt
    ####
    with open(gr._get_graph_label_file('AMP')) as f:
        lines = f.readlines()
    # The KMERS_graph_labels_AMP.txt file should only have as many lines as
    # there are genome files processed
    assert len(lines) == len(setup_snakefile)

    # Write out all maps and node_*.txt files
    gr.close()

    ####
    #   KMERS_node_labels.txt
    ####
    node_labels = np.loadtxt(gr.node_labels, dtype=int)
    assert 1 <= len(node_labels) <= gr.n

    ####
    #   KMERS_node_attributes.txt
    ####
    # The node attribute we use is the kmer, the max of which is 4^k
    node_attributes = np.loadtxt(gr.node_attributes, dtype=int)
    assert 1 <= len(node_attributes) <= gr.n

    ####
    #   Check that we can read all dictionary mapping files
    ####
    pd.read_csv(gr.file_mapping)
    pd.read_csv(gr.mic_mapping)
    pd.read_csv(gr.kmer_mapping)
    pd.read_csv(gr.label_mapping)
