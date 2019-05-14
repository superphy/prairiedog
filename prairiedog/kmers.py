import logging
import time
import os

log = logging.getLogger("prairiedog")


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

    def __str__(self):
        return "Kmers for file {} with {} contigs and K size {}".format(
            self.filepath, len(self.headers), self.k
        )

    def _load(self):
        # We have to merge multiline sequences
        seq = ""
        log.info(
            "Parsing Kmers for file {} with K size {} in pid {}".format(
                self.filepath, self.k, os.getpid()
            ))
        st = time.time()
        with open(self.filepath) as f:
            for line in f:
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
        en = time.time()
        log.info("Done creating {} in {} s".format(self, en - st))

    def _end_of_kmers(self) -> bool:
        return (self.pi + self.k) > len(self.sequences[self.li])

    @property
    def has_next(self) -> bool:
        """Returns true if the source file still has kmers.
        """
        is_last_sequence = self.li == (len(self.sequences) - 1)
        is_end_kmers = self._end_of_kmers()
        return not (is_last_sequence & is_end_kmers)

    @property
    def contig_has_next(self) -> bool:
        """Returns true if the current contig in a source file still has kmers.
        """
        return not self._end_of_kmers()

    def next(self) -> (str, str):
        """Emits the next kmer.
        """
        # Done.
        if not self.has_next:
            return "", ""

        # Move to next sequence.
        if not self.contig_has_next:
            self.li += 1
            self.pi = 0

        # K is greater than the size of the contig.
        if self.k > len(self.sequences[self.li]) - 1:
            log.warning(
                "Contig {} was shorter than K of {}, skipping...".format(
                    self.headers[self.li], self.k))
            self.li += 1
            self.pi = 0

        # Header + kmer
        header = self.headers[self.li]
        sl = self.sequences[self.li][self.pi: self.pi + self.k]

        # Increment
        self.pi += 1

        return header, sl
