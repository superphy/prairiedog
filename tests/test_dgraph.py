import subprocess


def test_dgraph_install():
    subprocess.run(['dgraph', '-h'], check=True, capture_output=True)
    assert True
