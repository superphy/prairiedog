import logging
import time
import os
import itertools
import typing

from prairiedog import recommended_procs

log = logging.getLogger("prairiedog")


GB_PER_PROC_KMERS = 5

recommended_procs_kmers = recommended_procs(GB_PER_PROC_KMERS)


def possible_kmers(k: int = 11) -> typing.Generator:
    """
    Utility function for generating all possible kmers of length k.
    :param k:
    :return:
    """
    kmers = (''.join(x) for x in itertools.product('ATCG', repeat=k))
    return kmers


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
        # Count the number of unique kmers
        self._n = 0  # non-unique kmers
        self.unique_kmers = self._count_unique()

    def __str__(self):
        return os.path.basename(self.filepath)

    def __len__(self):
        return self._n

    def _load(self):
        # We have to merge multiline sequences
        seq = ""
        log.debug(
            "Parsing Kmers for file {} with K size {} in pid {}".format(
                self.filepath, self.k, os.getpid()
            ))
        log.debug("Seeing current working directory as: {}".format(
            os.getcwd()))
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
        log.debug("Done creating {} in {} s".format(self, en - st))

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
                "Contig {} in file {} was shorter than K of {},"
                " skipping...".format(
                    self.headers[self.li], self.filepath, self.k))
            self.li += 1
            self.pi = 0

        # Header + kmer
        header = self.headers[self.li]
        sl = self.sequences[self.li][self.pi: self.pi + self.k]

        # Increment
        self.pi += 1

        return header, sl

    def reset(self):
        """
        Resets Kmer iterator.
        :return:
        """
        self.li = 0
        self.pi = 0

    def _count_unique(self) -> int:
        """
        Counts the number of unique kmers in the file.
        :return:
        """
        log.debug("Counting unique Kmers in file {}".format(self))
        st = set()
        c = 0
        while self.has_next:
            _, kmer = self.next()
            st.add(kmer)
            c += 1
        self.reset()
        uc = len(st)
        log.debug("Counted {} unique Kmers in file {}".format(uc, self))
        self._n = c
        return uc
