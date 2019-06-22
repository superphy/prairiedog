import subprocess

import pandas as pd


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

    assert True
