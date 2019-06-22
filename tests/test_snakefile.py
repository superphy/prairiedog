import subprocess


def test_snakefile_dryrun():
    subprocess.run(['snakemake', '-n', '-r'], check=True)
    assert True

def test_snakefile_full(setup_snakefile):
    # Set the config MIC csv to use our test one
    mic_csv = 'tests/public_mic_class_dataframe_test.csv'

    # Use Snakemake to drive graph creation for two files
    subprocess.run('snakemake --config graph_labels={}'.format(mic_csv),
                   check=True, shell=True)

    assert True


def test_snakefile_full_pyinstrument(setup_snakefile):
    # Set the config MIC csv to use our test one
    mic_csv = 'tests/public_mic_class_dataframe_test.csv'

    # Use Snakemake to drive graph creation for two files
    subprocess.run(
        'snakemake --config graph_labels={} --config pyinstrument=True'.format(
            mic_csv),
        check=True, shell=True)

    assert True
