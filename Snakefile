import pickle
import pathlib
import os

from prairiedog.kmers import Kmers
from prairiedog.networkx_graph import NetworkXGraph
from prairiedog.graph_ref import GraphRef
from prairiedog.subgraph_ref import SubgraphRef

K = 11

INPUTS = [f.split('.')[0] for f in os.listdir('samples/')
           if f.endswith(('.fna', '.fasta', '.fa'))
]

rule all:
    input:
         'output/KMERS_A.txt'

rule kmers:
    input:
        'samples/{sample}.fasta'
    output:
        'outputs/kmers/{sample}.pkl'
    run:
        pathlib.Path('outputs/kmers/').mkdir(parents=True, exist_ok=True)
        km = Kmers({input},{K})
        pickle.dump(km, open({output},'wb'))

rule offset:
    input:
        expand('outputs/kmers/{input}.pkl', input=INPUTS)
    output:
        'outputs/kmers/offsets.pkl', 'outputs/graphref.pkl'
    run:
        offsets = {}
        max_n = 4 ** {K} * len({INPUTS})
        gr = GraphRef(max_n)
        for kmf in {input}:
            km = pickle.load(open(kmf,'rb'))
            offset = gr.node_id_count
            offsets[{kmf}] = offset
            gr.incr_node_id(km)
        pickle.dump(offsets, open('outputs/kmers/offsets.pkl','wb'))
        pickle.dump(gr, open('outputs/graphref.pkl','wb'))

rule subgraphs:
    input:
        kmf='outputs/kmers/{sample}.pkl',
        offsets='outputs/kmers/offsets.pkl'
    output:
        'output/subgraphs/{sample}.pkl'
    run:
        offsets = pickle.load(open({kmf}, 'rb'))
        offset = offsets[{kmf}]
        km = pickle.load(open({kmf},'rb'))
        sg = SubgraphRef(offset, km, NetworkXGraph())
        pickle.dump(sg, open({output},'wb'))

rule graph:
    input:
        subgraphs=expand('output/subgraphs/{input}.pkl', input=INPUTS),
        graphref='outputs/graphref.pkl'
    output:
        'output/KMERS_A.txt'
    run:
        gr = pickle.load(open({graphref}))
        for sgf in {subgraphs}:
            sg = pickle.load(open(sgf,'rb'))
            gr.append(sg)
        gr.close()
