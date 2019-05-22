import pathlib
import os

import dill

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
        'outputs/kmers/offsets.pkl', 'outputs/graphref.pkl'
    run:
        offsets = {}
        max_n = 4 ** K * len(INPUTS)
        gr = GraphRef(max_n, 'outputs', MIC_CSV)
        for kmf in input:
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
        'outputs/KMERS_A.txt', 'outputs/graphref_final.pkl'
    run:
        gr = dill.load(open(input.graphref, 'rb'))
        for sgf in input.subgraphs:
            sg = dill.load(open(sgf,'rb'))
            gr.append(sg)
        dill.dump(gr, open(output[1], 'wb'), protocol=4)
        gr.close()

rule clean:
    shell:
        "rm -rf outputs/"
