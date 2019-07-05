import pathlib
import os
import shutil
import subprocess
import psutil

import dill
import pandas as pd
import numpy as np
from contextlib import contextmanager

from prairiedog.profiler import Profiler
from prairiedog.kmers import Kmers
from prairiedog.networkx_graph import NetworkXGraph
from prairiedog.graph_ref import GraphRef
from prairiedog.subgraph_ref import SubgraphRef
from prairiedog.lemon_graph import LGGraph, DB_PATH
from prairiedog.dgraph import Dgraph

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

rule preload:
    output:
        'outputs/preloaded.txt'
    run:
        if config['backend'] == 'dgraph':
            dg = Dgraph()
            dg.preload(K)
        open(output[0], 'w').close()

rule pangenome:
    input:
        'outputs/kmers/{input}.pkl',
        'outputs/preloaded.txt'
    output:
        'outputs/pangenome_{input}.g'
    run:
        # Remove old memory usage logs
        # sizes = []
        # try:
        #     os.remove('outputs/sizes_{}.txt'.format(input[0]))
        # except:
        #     pass

        # Setup graph backend
        gr = GraphRef(MIC_CSV)
        if config['backend'] == 'networkx':
            print("Using NetworkX as graph backend")
            sg = SubgraphRef(NetworkXGraph())
        elif config['backend'] == 'lemongraph':
            print("Using LemonGraph as graph backend")
            sg = SubgraphRef(LGGraph())
        elif config['backend'] == 'dgraph':
            print("Using Dgraph as graph backend")
            sg = SubgraphRef(Dgraph())
        else:
            raise Exception("No graph backend found")

        # Setup pyinstrument profiler
        if config['pyinstrument'] is True:
            print("Setting up pyinstrument profiler...")
            profiler = Profiler()
            profiler.start()
        else:
            profiler = None

        # Note that start=1 is only for the index, sgf still starts at
        # position 0
        # for index, kmf in enumerate(input, start=1):
        # print("rule 'pangenome' on Kmer {} / {}".format(
        #     index, len(input)))
        km = dill.load(open(input[0],'rb'))
        gr.index_kmers(km)
        sg.update_graph(km, gr)
        if config['backend'] in ('lemongraph', 'dgraph'):
            sg.save(output[0])

        # Calculate rough memory usage
        pid = os.getpid()
        py = psutil.Process(pid)
        sz = py.memory_info()[0]/2.**30
        # sizes.append(sz)
        print("Current process memory usage is {} GB".format(sz))
        # with open('outputs/sizes.txt', 'a') as f:
        #     f.write('{}\n'.format(sz))

        # It seems the km object is being kept in memory for too long
        del km

        # Stop pyinstrument profiler
        if config['pyinstrument'] is True:
            profiler.stop()
            print(profiler.output_text(unicode=True, color=True))

        print("rule 'pangenome' found max_num_nodes to be {}".format(
            gr.max_num_nodes))
        dill.dump(gr,
                    open('outputs/graphref.pkl','wb'), protocol=4)
        if config['backend'] == 'lemongraph':
            shutil.copy2(DB_PATH, output[0])
        elif config['backend'] == 'dgraph':
            open(output[0], 'w').close()
        else:
            sg.save(output[0])

        # dill.dump(sizes, open('outputs/sizes.pkl', 'wb'))

rule done:
    input:
        expand('outputs/pangenome_{input}.g', input=INPUTS)
    output:
        'outputs/pangenome.g'
    run:
        open(output[0], 'w').close()

rule clean:
    shell:
        "rm -rf outputs/"
