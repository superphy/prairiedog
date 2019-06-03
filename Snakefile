import pathlib
import os
import shutil
import subprocess

import dill
import pandas as pd
import numpy as np
from contextlib import contextmanager

from prairiedog.kmers import Kmers
from prairiedog.networkx_graph import NetworkXGraph
from prairiedog.graph_ref import GraphRef
from prairiedog.subgraph_ref import SubgraphRef

configfile: "config.yaml"

K = config["k"]
INPUTS = [f.split('.')[0] for f in os.listdir(config["samples"])
           if f.endswith(('.fna', '.fasta', '.fa'))
]
MIC_CSV = config["graph_labels"]
MIC_COLUMNS = set(pd.read_csv(MIC_CSV).columns)
MIC_COLUMNS.remove('run')

###################
# Graphing steps
###################

rule all:
    input:
         expand('outputs/subgraphs/{input}.g', input=INPUTS)

rule kmers:
    input:
        'samples/{sample}.fasta'
    output:
        'outputs/kmers/{sample}.pkl'
    run:
        pathlib.Path('outputs/kmers/').mkdir(parents=True, exist_ok=True)
        km = Kmers(input[0],K)
        dill.dump(km, open(output[0],'wb'))

rule index:
    input:
        expand('outputs/kmers/{input}.pkl', input=INPUTS)
    output:
        'outputs/graphref.pkl',
    run:
        gr = GraphRef(MIC_CSV)
        # Note that start=1 is only for the index, sgf still starts at
        # position 0
        for index, kmf in enumerate(input, start=1):
            print("rule 'offset' on Kmer {} / {}".format(
                index, len(input)))
            km = dill.load(open(kmf,'rb'))
            gr.index_kmers(km)
            # It seems the km object is being kept in memory for too long
            del km
        print("rule 'offset' found max_num_nodes to be {}".format(
            gr.max_num_nodes))
        dill.dump(gr,
                    open('outputs/graphref.pkl','wb'), protocol=4)

rule subgraphs:
    input:
        kmf='outputs/kmers/{sample}.pkl',
        gr='outputs/graphref.pkl',
    output:
        'outputs/subgraphs/{sample}.g'
    run:
        pathlib.Path('outputs/subgraphs/').mkdir(parents=True, exist_ok=True)
        km = dill.load(open(input.kmf,'rb'))
        gr = dill.load(open(input.gr, 'rb'))
        sg = SubgraphRef(km, NetworkXGraph(), gr, target='AMP')
        sg.save(output[0])

###################
# Training steps
###################

@contextmanager
def _setup_training(mic_label: str) -> int:
    dst = pathlib.Path('diffpool/data/KMERS/KMERS_graph_labels.txt')
    print("Copying {} to {}".format(graph, dst))
    shutil.copy2(mic_label, dst)
    n = len(
        np.unique(
            np.loadtxt(dst, dtype=int)
        )
    )
    yield n
    os.remove(dst)

def train_model(target):
    print("Currently training for {}".format(target))
    with _setup_training(target) as n:
        subprocess.run(
            'cd diffpool/ && python -m train --bmname=KMERS --assign-ratio=0.1 \
            --hidden-dim=30 --output-dim=30 --cuda=0 --num-classes={} \
            --method=soft-assign --benchmark-iterations=1'.format(n),
            shell=True,
            check=True)

rule move:
    input:
        a='outputs/KMERS_A.txt',
        gi='outputs/KMERS_graph_indicator.txt',
        na='outputs/KMERS_node_attributes.txt',
        nl='outputs/KMERS_node_labels.txt'
    output:
        'diffpool/data/KMERS/KMERS_A.txt',
        'diffpool/data/KMERS/KMERS_graph_indicator.txt',
        'diffpool/data/KMERS/KMERS_node_attributes.txt',
        'diffpool/data/KMERS/KMERS_node_labels.txt',
    run:
        # File moving
        directory = pathlib.Path('diffpool/data/KMERS/')
        directory.mkdir(parents=True, exist_ok=True)
        shutil.move(input.a, directory)
        shutil.move(input.gi, directory)
        shutil.move(input.na, directory)
        shutil.move(input.nl, directory)

rule train:
    input:
        mic_labels=expand(
            'outputs/KMERS_graph_labels_{target}.txt', target=MIC_COLUMNS),
        outputs=rules.move.output
    output:
        'diffpool/results/'
    run:
        # Actual train
        c = 1
        l = len(input.mic_labels)
        for mic_label in input.mic_labels:
            print("{}/{} : Currently training for {}".format(
                c, l, mic_label))
            train_model(mic_label)

rule clean:
    shell:
        "rm -rf outputs/"
