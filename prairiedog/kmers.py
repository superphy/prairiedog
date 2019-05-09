class Kmers:
    # TODO: This really should all be read from a buffer instead of directly into memory
    def __init__(self, filepath):
        self.filepath = filepath
        self.headers = []
        self.sequences = []

    def load(self):
        with open(self.filepath) as f:
            lines = f.readlines()
        for line in lines:
            if line.startswith(">"):
                self.headers.append(line)
            else:
                self.sequences.append(line)
