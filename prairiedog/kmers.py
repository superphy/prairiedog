class Kmers:
    # TODO: This really should all be read from a buffer instead of directly
    #  into memory
    def __init__(self, filepath, k=11):
        self.filepath = filepath
        # Sequences
        self.headers = []
        self.sequences = []
        # Ints
        self.li = 0  # Current line
        self.pi = 0  # Current position in the line
        self.k = k
        # Load
        self._load()

    def _load(self):
        # We have to merge multiline sequences
        seq = ""
        with open(self.filepath) as f:
            lines = f.readlines()
        for line in lines:
            ln = line.rstrip()
            if ln.startswith(">"):
                # If we're at a new contig append the sequence
                if len(seq) != 0:
                    self.sequences.append(seq)
                    seq = ""
                # Always append the header
                self.headers.append(ln)
            else:
                # We're in some part of the sequence, continue to concat it
                seq += ln
        # Always append the last sequence
        self.sequences.append(seq)

    def _end_of_kmers(self) -> bool:
        return (self.pi + self.k) > len(self.sequences[self.li])

    def has_next(self) -> bool:
        """Returns true if the source file still has kmers.
        """
        is_last_sequence = self.li == (len(self.sequences) - 1)
        is_end_kmers = self._end_of_kmers()
        return not (is_last_sequence & is_end_kmers)

    def contig_has_next(self) -> bool:
        """Returns true if the current contig in a source file still has kmers.
        """
        return not self._end_of_kmers()

    def next(self) -> (str, str):
        """Emits the next kmer.
        """
        # Done.
        if not self.has_next():
            return "", ""

        # Move to next sequence.
        if not self.contig_has_next():
            self.li += 1
            self.pi = 0

        # K is greater than the size of the contig.
        if self.k > len(self.sequences[self.li]) - 1:
            # TODO: warn
            self.li += 1
            self.pi = 0

        # Header + kmer
        header = self.headers[self.li]
        sl = self.sequences[self.li][self.pi: self.pi + self.k]

        # Increment
        self.pi += 1

        return header, sl
