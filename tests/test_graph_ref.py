import subprocess
import pickle

import pandas as pd
import numpy as np


def test_pandas_read_mic_csv():
    pd.read_csv('tests/public_mic_class_dataframe_test.csv')
    pd.read_csv('samples/public_mic_class_dataframe.csv')
    assert True
