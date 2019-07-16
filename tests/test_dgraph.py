import subprocess


def test_dgraph_install():
    subprocess.run('dgraph -h', shell=True, check=True)
    assert True
