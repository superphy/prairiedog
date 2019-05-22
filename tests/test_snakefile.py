import subprocess


def test_snakefile():
    subprocess.run(['snakemake', '-n', '-r'], check=True)
    assert True
