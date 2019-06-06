import pathlib
import os
import shutil
import subprocess
import psutil

import dill
import pandas as pd
import numpy as np
from contextlib import contextmanager

from prairiedog.kmers import Kmers
from prairiedog.networkx_graph import NetworkXGraph
from prairiedog.graph_ref import GraphRef
from prairiedog.subgraph_ref import SubgraphRef
from prairiedog.dgl_graph import DGLGraph

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
         'outputs/pangenome.g'

rule kmers:
    input:
        'samples/{sample}.fasta'
    output:
        'outputs/kmers/{sample}.pkl'
    run:
        pathlib.Path('outputs/kmers/').mkdir(parents=True, exist_ok=True)
        km = Kmers(input[0],K)
        dill.dump(km, open(output[0],'wb'))

rule pangenome:
    input:
        expand('outputs/kmers/{input}.pkl', input=INPUTS)
    output:
        'outputs/graphref.pkl',
        'outputs/pangenome.g'
    run:
        try:
            os.remove('outputs/sizes.txt')
        except:
            pass
        sizes = []
        gr = GraphRef(MIC_CSV)
        if config['backend'] == 'networkx':
            print("Using NetworkX as graph backend")
            sg = SubgraphRef(NetworkXGraph())
        else:
            print("Using DGL as graph backend")
            sg = SubgraphRef(DGLGraph(len(input)))
        pathlib.Path('outputs/subgraphs/').mkdir(parents=True, exist_ok=True)
        # Note that start=1 is only for the index, sgf still starts at
        # position 0
        for index, kmf in enumerate(input, start=1):
            print("rule 'pangenome' on Kmer {} / {}".format(
                index, len(input)))
            km = dill.load(open(kmf,'rb'))
            gr.index_kmers(km)
            sg.update_graph(km, gr)

            # Calculate rough memory usage
            pid = os.getpid()
            py = psutil.Process(pid)
            sz = py.memory_info()[0]/2.**30
            sizes.append(sz)
            print("Current graph size is {} GB".format(sz))
            with open('outputs/sizes.txt', 'a') as f:
                f.write('{}\n'.format(sz))

            # It seems the km object is being kept in memory for too long
            del km
        print("rule 'pangenome' found max_num_nodes to be {}".format(
            gr.max_num_nodes))
        dill.dump(gr,
                    open('outputs/graphref.pkl','wb'), protocol=4)
        sg.save(output[1])
        dill.dump(sizes, open('outputs/sizes.pkl', 'wb'))

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
