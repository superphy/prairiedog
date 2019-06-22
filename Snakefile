import pathlib
import os
import shutil
import subprocess
import psutil

import dill
import pandas as pd
import numpy as np
from contextlib import contextmanager
from pyinstrument import Profiler

from prairiedog.kmers import Kmers
from prairiedog.networkx_graph import NetworkXGraph
from prairiedog.graph_ref import GraphRef
from prairiedog.subgraph_ref import SubgraphRef
from prairiedog.dgl_graph import DGLGraph
from prairiedog.lemon_graph import LGGraph, DB_PATH

configfile: "config.yaml"

K = config["k"]
INPUTS = [os.path.splitext(f)[0] for f in os.listdir(config["samples"])
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
        # Remove old memory usage logs
        sizes = []
        try:
            os.remove('outputs/sizes.txt')
        except:
            pass

        # Setup graph backend
        gr = GraphRef(MIC_CSV)
        if config['backend'] == 'networkx':
            print("Using NetworkX as graph backend")
            sg = SubgraphRef(NetworkXGraph())
        elif config['backend'] == 'lemongraph':
            print("Using LemonGraph as graph backend")
            sg = SubgraphRef(LGGraph())
        else:
            print("Using DGL as graph backend")
            sg = SubgraphRef(DGLGraph(
                n_labels=len(input),
                n_nodes=4**K + 20 # We have some odd contigs that use N
            ))

        # Setup pyinstrument profiler
        if config['pyinstrument'] is True:
            print("Setting up pyinstrument profiler...")
            profiler = Profiler()
            profiler.start()
        else:
            profiler = None

        # Note that start=1 is only for the index, sgf still starts at
        # position 0
        for index, kmf in enumerate(input, start=1):
            print("rule 'pangenome' on Kmer {} / {}".format(
                index, len(input)))
            km = dill.load(open(kmf,'rb'))
            gr.index_kmers(km)
            sg.update_graph(km, gr)
            if config['backend'] == 'lemongraph':
                sg.save(output[1])

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

        # Stop pyinstrument profiler
        if config['pyinstrument'] == "True":
            profiler.stop()
            print(profiler.output_text(unicode=True, color=True))

        print("rule 'pangenome' found max_num_nodes to be {}".format(
            gr.max_num_nodes))
        dill.dump(gr,
                    open('outputs/graphref.pkl','wb'), protocol=4)
        if config['backend'] == 'lemongraph':
            shutil.copy2(DB_PATH, output[1])
        else:
            sg.save(output[1])

        dill.dump(sizes, open('outputs/sizes.pkl', 'wb'))

rule clean:
    shell:
        "rm -rf outputs/"
