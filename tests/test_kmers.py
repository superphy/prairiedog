#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `prairiedog` package."""

import pytest

from prairiedog import kmers


@pytest.fixture
def example_kmers(f="tests/172.fa"):
    km = kmers.Kmers(f)
    return km


def test_kmers_load():
    km = example_kmers()
    assert km.headers[0] == ">gi|1062504329|gb|CP014670.1| Escherichia coli strain CFSAN004177, complete genome"


def test_kmers_next():
    km = example_kmers()
    header, kmer = km.next()
    assert header == ">gi|1062504329|gb|CP014670.1| Escherichia coli strain CFSAN004177, complete genome"
    assert kmer == "TCGCTTTCGTT"
    header, kmer = km.next()
    assert header == ">gi|1062504329|gb|CP014670.1| Escherichia coli strain CFSAN004177, complete genome"
    assert kmer == "CGCTTTCGTTC"


def test_kmers_index():
    km = example_kmers("tests/ECI-2866_lcl.fasta")
    assert len(km.headers) == 297
    assert len(km.sequences) == 297
    assert km.headers[0] == ">lcl|ECI-2866|NODE_177_length_532_cov_12.8938_ID_353"
    assert km.headers[1] == ">lcl|ECI-2866|NODE_222_length_438_cov_0.710611_ID_443"
    assert km.sequences[0] == "AACGCGCACTGACGTGAGGCCAGCGTACAGGCCGGATTATCGACATATTTCTGACAGGTGCCGTTATCTGCGGACTGTGTGACATATTTATCCCGGTATGCCCAGCACGCCTGTGTGATGCTCCAGGGTTTACCTTCCATCACACCTGTTTTCGTCCCCCCCGGCTCTGAACACTCAGTACCTTTCAGCACGCCATCCGCTTTATTAAACGGACAACTCTCCACCCACTCCACCCGTGGAACCCATTCCTTATCACGGACCTTCATCCTGAGTTTCAGCGTAAAGGTGGAAGCACCACTGACAAGCGATTCATAGACCATCCTGTCACCATTCCCGTGCGGGAGGCAATTACCGTTTGCAGTACAGCCACTACCGATCAGAACCTGCCCCTGTGTGACAGAAAACCCGGAGGGCACAGGTATGGTGAAAGTCCCGCTCTGTCCTGTAACCAGTTGCACATTAAAGGCTGTGTTCATAAAGTCGTAACGGGAGTTAAGGAAATATAGCCCTGCATGAGCCGACAGCGAGGCAC"
    assert km.sequences[1] == "ACATCGTGCCGCATTGTTGGCAGAGGGAATTCCTTTTCATTGCTTTTATTATCCCTGTGTTAGTGAAATACTACGTTAGGGTTTGGAACACGTAAGAAAAATGGCGTTGTCAATGGGATTGTTTTTTTTTTATGCCGGTCAGATCTCAAAAACTAGGCCAGAGATCAATTCTACTTGACCTCATGACAGTTTACTGCCGCTGCTGCCGGAATCCAAATCTCGTGGTATCCTAACTCAAGGAGTCGGCATGAAGTCCATCGAAGCATATTTTCTGTTCATCAGGTATTGACTAGTGACTCTGCAAGGACAAATCACCTTACTACATCCTGGTCCATGGTGAAGTCTAGCTTTGATACCTTGAGTTGTCCATTCCCGGAAATGCACCTCCGGGCCAGGGGTGCTCGCTCTGACCTTCGTGTCCCATGGAACTTCAGCCAG"

    n = len(km.sequences[0])
    assert n == 532
    assert km.sequences[0][n-1] == "C"
    assert km.sequences[0][n-2] == "A"
    assert km.sequences[0][n-3] == "C"


def test_kmers_index_end():
    """Checks end case.
    """
    km = example_kmers("tests/ECI-2866_lcl.fasta")
    header, kmer = km.next()
    while km.has_next():
        header, kmer = km.next()
    assert header == ">lcl|ECI-2866|NODE_22_length_88582_cov_33.0406_ID_43"
    assert kmer == "TACGGATTCTT"


def test_kmers_index_diff():
    """Checks last of a contig before switching.
    """
    km = example_kmers("tests/GCA_900015695.1_ED647_contigs_genomic.fna")
    header, kmer = km.next()
    assert header == ">FAVS01000269.1 Escherichia coli strain ED647 genome assembly, contig: out_269, whole genome shotgun sequence"
    while km.contig_has_next():
        header, kmer = km.next()
    assert header == ">FAVS01000269.1 Escherichia coli strain ED647 genome assembly, contig: out_269, whole genome shotgun sequence"
    assert kmer == "TACTGCTACTG"
