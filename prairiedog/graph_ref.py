import os
import pathlib
import datetime
import logging
import abc
import time
import pickle

import numpy as np
import pandas as pd

import prairiedog.config as config
from prairiedog.kmers import Kmers
from prairiedog.graph import Graph

log = logging.getLogger("prairiedog")


class GRef(metaclass=abc.ABCMeta):
    """
    Base class for graph reference classes.
    """
    @staticmethod
    def _upsert_map(d: dict, value: str, prev_value: int = 0) -> int:
        if value not in d:
            # This starts the MIC label at 1 or prev_value + 1
            d[value] = len(d) + prev_value + 1
        return d[value]


class SubgraphRef(GRef):
    """
    Helper for creating a NetworkX graph, created for each genome file.
    """
    def __init__(self, prev_node_id: int, km: Kmers, graph: Graph):
        """
        """
        # This should already be assigned by the previous Kmer
        self.prev_node_id = prev_node_id
        self.km = km
        self.graph = graph
        self.subgraph_kmer_map = {}
        self._create_graph()

    def _create_graph(self) -> int:
        log.debug(
            "Starting to graph {} in pid {}".format(self.km, os.getpid()))
        st = time.time()
        c = 0
        while self.km.has_next:
            header1, kmer1 = self.km.next()
            # Create the first node
            node1_id = self._upsert_map(
                self.subgraph_kmer_map, kmer1, self.prev_node_id)
            self.graph.upsert_node(node1_id)
            c += 1
            # The same contig still has a kmer
            while self.km.contig_has_next:
                header2, kmer2 = self.km.next()
                # Create the second node
                node2_id = self._upsert_map(
                    self.subgraph_kmer_map, kmer2, self.prev_node_id)
                self.graph.upsert_node(node2_id)
                # Create an edge
                self.graph.add_edge(node1_id, node2_id)
                # Set node1_id to node2_id
                node1_id = node2_id
                c += 1
            # At this point, we're out of kmers on that contig
            # The loop will check if there's still kmers, and reset kmer1
        en = time.time()
        log.debug("Done graphing {}, covering {} kmers in {} s".format(
            self.km, c, en-st))
        return c


class GraphRef(GRef):
    """
    Helper class to track node ints, etc.
    """
    def __init__(self):
        self.node_id_count = 0
        self.MIC_DF = pickle.load(
            open(config.MIC_DF, 'rb')
        )
        self.MIC_COLUMNS = self.MIC_DF.columns
        # Reference for all files encountered
        self.file_map = {}
        self.mic_maps = {
            mic: {} for mic in self.MIC_COLUMNS
        }
        self.kmer_map = {}
        # NumPy arrays
        self.node_label_array = None
        self.node_attributes_array = None
        # Output folders
        pf = '{date:%Y-%m-%d_%H-%M-%S}'.format(date=datetime.datetime.now())
        self.output_folder = 'output/{}'.format(pf)
        self._setup_folders()
        # Calculate constants for output sizes
        self.max_n = (4**config.K)*config.INPUT_FILES
        self.N = len(config.INPUT_FILES)
        # Output files
        self.graph_indicator = os.path.join(
            self.output_folder, 'KMERS_graph_indicator.txt')
        self.node_labels = os.path.join(
            self.output_folder, 'KMERS_node_labels.txt')
        self.node_attributes = os.path.join(
            self.output_folder, 'KMERS_node_attributes.txt')
        # For user reference, not used in models
        self.file_mapping = os.path.join(
            self.output_folder, 'KMERS_file_mapping.txt')
        self.mic_mapping = os.path.join(
            self.output_folder, 'KMERS_mic_mapping.txt')
        self.kmer_mapping = os.path.join(
            self.output_folder, 'KMERS_kmer_mapping.txt')

    def init_node_arrays(self, n: int):
        log.debug("Initializing NumPy arrays to length {}".format(n))
        self.node_label_array = np.empty(n, dtype=int)
        self.node_attributes_array = np.empty(n, dtype=int)

    def _setup_folders(self):
        pathlib.Path(self.output_folder).mkdir(parents=True, exist_ok=True)

    def _get_graph_label_file(self, label: str):
        """
        Graph labels are a bit unique in that we make predictions for all drugs
        we have MIC data for. This means we'll eventually have to train per
        drug.
        :param label:
        :return:
        """
        return os.path.join(
            self.output_folder, 'KMERS_graph_labels_{}.txt'.format(label))

    def write_graph_label(self,  km: Kmers):
        short_name = os.path.basename(km.filepath).split('.')[0]
        series = self.MIC_DF.loc[short_name, :]
        for label in self.MIC_COLUMNS:
            mic = series[label]
            graph_label_file = self._get_graph_label_file(label)
            mic_map = self.mic_maps[label]
            with open(graph_label_file, 'a') as f:
                f.write('{}\n'.format(self._upsert_map(mic_map, mic)))

    def incr_node_id(self, km: Kmers):
        """
        When we increment the currently assigned node id, we know a few things
        about how the eventual graph will be constructions. Namely:
        - we know that for lines [i to i+km.unique_kmers] (the unique nodes
            assigned to the graph) the graph_indicator id will be the same
        - for line i element of N, we'll have given MIC values
        Other data, namely node labels and node attributes, will not be known
        until the subgraph is constructed and we have assigned a given genome
        file's kmer:node_id.
        :param km:
        :return:
        """
        self.node_id_count += km.unique_kmers
        i = self._upsert_map(self.file_map, km.filepath)

        # Call to write out a KMERS_graph_indicator.txt file
        with open(self.graph_indicator, 'a') as f:
            f.write('{}\n'.format(i))

        # Call to write out KMERS_graph_labels_{}.txt files
        self.write_graph_label(km)

    def _find_kmer_label(self, kmer: str):
        pass

    def record_node_labels(self, pos_id: int, kmer: str):
        """
        Needs to cross-ref kmer against a label of some sort
        :param pos_id:
        :param kmer:
        :return:
        """
        pass

    def record_node_attributes(self, pos_id: int, kmer: str):
        """
        Record the kmer as an attribute for node i.
        :param pos_id:
        :param kmer:
        :return:
        """
        kmer_id = self._upsert_map(self.kmer_map, kmer)
        # Node IDs start at 1
        self.node_attributes_array[pos_id] = kmer_id

    def append(self, subgraph: SubgraphRef) -> int:
        """
        Appends to relevant files. We have to do some mapping to resolve
        strings and other variables into incrementing ints for the models.
        This function is called to get the node_id for NetworkX.
        """
        # So we can map node_id : kmer
        inverted = {
            value: key
            for key, value in subgraph.subgraph_kmer_map.items()}
        for node_id in subgraph.graph.nodes:
            kmer = inverted[node_id]
            # Node IDs start at 1
            pos_id = node_id - 1
            self.record_node_labels(pos_id, kmer)
            self.record_node_attributes(pos_id, kmer)

    def close(self):
        """
        Make sure to write out all mappings for reference
        :return:
        """
        pass
        # def _write(fl, di):
        #     with open(fl, 'a') as fil:
        #         for k, v in di:
        #             fil.write('{}, {}\n'.format(k, v))
        #
        # _write(self.file_mapping, self.file_map)
        # _write(self.mic_mapping, self.mic_map)
        # _write(self.kmer_mapping, self.kmer_map)
