import os

K = 11
INPUT_DIRECTORY = 'samples/'
MIC_DF = '../samples/public_mic_class_dataframe.pkl'

def _input_files():
    fls = [os.path.join(INPUT_DIRECTORY, f) for f in os.listdir(INPUT_DIRECTORY)
           if f.endswith(('.fna', '.fasta', '.fa'))]
    return fls


INPUT_FILES = _input_files()
