import pathlib
import os
import shutil

import dill
import pandas as pd

from prairiedog.profiler import Profiler
from prairiedog.kmers import Kmers
from prairiedog.networkx_graph import NetworkXGraph
from prairiedog.graph_ref import GraphRef
from prairiedog.subgraph_ref import SubgraphRef
from prairiedog.lemon_graph import LGGraph, DB_PATH
from prairiedog.dgraph import DgraphBulk, port
from prairiedog.dgraph_bundled import DgraphBundled, offset
from dgraph.bulk import dgraph_bulk_cmd

configfile: "config.yaml"

K = config["k"]
INPUTS = [os.path.splitext(f)[0] for f in os.listdir(config["samples"])
           if f.endswith(('.fna', '.fasta', '.fa'))
]
MIC_CSV = config["graph_labels"]
if os.path.isfile(MIC_CSV):
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
        'outputs/kmers/{input}.pkl'
    output:
        'outputs/pangenome_{input}.g'
    run:
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
            sg = SubgraphRef(DgraphBulk())
        else:
            raise Exception("No graph backend found")

        # Setup pyinstrument profiler
        if config['pyinstrument'] is True:
            print("Setting up pyinstrument profiler...")
            profiler = Profiler()
            profiler.start()
        else:
            profiler = None

        # Main graphing step
        km = dill.load(open(input[0],'rb'))
        gr.index_kmers(km)
        sg.update_graph(km, gr)
        if config['backend'] in ('lemongraph', 'dgraph'):
            sg.save(output[0])

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

rule done:
    input:
        expand('outputs/pangenome_{input}.g', input=INPUTS)
    output:
        'outputs/pangenome.g'
    run:
        open(output[0], 'w').close()

###########
# Dgraph specific rules
###########

rule preload:
    output:
        'outputs/samples/kmers.rdf'
    run:
        dg = DgraphBulk()
        print("Trying to create rdf for all possible k-mers...")
        dg.preload(K)
        print("Done creating rdf for all possible k-mers.")
        print("Trying to save rdf to file {}".format(output[0]))
        dg.save(output[0])

rule dgraph:
    input:
        'outputs/pangenome.g',
        'outputs/samples/kmers.rdf'
    output:
        'outputs/dgraph.done'
    run:
        # Create a reference to a running Dgraph instance
        dg = DgraphBundled(delete=False, output_folder='outputs/dgraph/')
        # Execute dgraph bulk
        p = port('ZERO', offset)
        shell(dgraph_bulk_cmd(zero_port=p))
        # Create the done file
        open(output[0], 'w').close()

rule clean:
    shell:
        "rm -rf outputs/"
