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
MIC_COLUMNS = pd.read_csv(MIC_CSV).columns

###################
# Graphing steps
###################

rule all:
    input:
         'outputs/KMERS_A.txt'

rule kmers:
    input:
        'samples/{sample}.fasta'
    output:
        'outputs/kmers/{sample}.pkl'
    run:
        pathlib.Path('outputs/kmers/').mkdir(parents=True, exist_ok=True)
        km = Kmers(input[0],K)
        dill.dump(km, open(output[0],'wb'))

rule offset:
    input:
        expand('outputs/kmers/{input}.pkl', input=INPUTS)
    output:
        'outputs/kmers/offsets.pkl',
        'outputs/graphref.pkl',
        'outputs/KMERS_graph_indicator.txt'
    run:
        offsets = {}
        max_n = 4 ** K * len(INPUTS)
        gr = GraphRef(max_n, 'outputs', MIC_CSV)
        # Note that start=1 is only for the index, sgf still starts at
        # position 0
        for index, kmf in enumerate(input, start=1):
            print("rule 'offset' on Kmer {} / {}".format(
                index, len(input)))
            km = dill.load(open(kmf,'rb'))
            offset = gr.node_id_count
            offsets[kmf] = offset
            gr.incr_node_id(km)
            # It seems the km object is being kept in memory for too long
            del km
        dill.dump(offsets,
                    open('outputs/kmers/offsets.pkl','wb'), protocol=4)
        dill.dump(gr,
                    open('outputs/graphref.pkl','wb'), protocol=4)

rule subgraphs:
    input:
        kmf='outputs/kmers/{sample}.pkl',
        offsets='outputs/kmers/offsets.pkl'
    output:
        'outputs/subgraphs/{sample}.pkl'
    run:
        pathlib.Path('outputs/subgraphs/').mkdir(parents=True, exist_ok=True)
        offsets = dill.load(open(input.offsets, 'rb'))
        offset = offsets[input.kmf]
        km = dill.load(open(input.kmf,'rb'))
        sg = SubgraphRef(offset, km, NetworkXGraph())
        dill.dump(sg, open(output[0],'wb'))

rule graph:
    input:
        subgraphs=expand('outputs/subgraphs/{input}.pkl', input=INPUTS),
        graphref='outputs/graphref.pkl'
    output:
        'outputs/KMERS_A.txt',
        'outputs/graphref_final.pkl',
        'outputs/KMERS_node_attributes.txt',
        'outputs/KMERS_node_labels.txt'
    run:
        gr = dill.load(open(input.graphref, 'rb'))
        # Note that start=1 is only for the index, sgf still starts at
        # position 0
        for index, sgf in enumerate(input.subgraphs, start=1):
            print("rule 'graph' on subgraph {} / {}".format(
                index, len(input.subgraphs)))
            sg = dill.load(open(sgf,'rb'))
            gr.append(sg)
        dill.dump(gr, open(output[1], 'wb'), protocol=4)
        gr.close()

###################
# Training steps
###################

@contextmanager
def _setup_training(target: str) -> int:
    graph_labels = pathlib.Path(
        'outputs/KMERS_graph_labels_{}.txt'.format(target))
    dst = pathlib.Path('diffpool/data/KMERS/KMERS_graph_labels.txt')
    print("Copying {} to {}".format(graph, dst))
    shutil.copy2(graph_labels, dst)
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
            'python -m train --bmname=KMERS --assign-ratio=0.1 \
            --hidden-dim=30 --output-dim=30 --cuda=0 --num-classes={} \
            --method=soft-assign'.format(n), shell=True)

rule train:
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
        # Actual train
        for target in MIC_COLUMNS:
            print("Currently training for {}".format(target))
            train_model(target)

rule clean:
    shell:
        "rm -rf outputs/"
