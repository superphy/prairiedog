import os
import pathlib
import datetime
import logging
import time

import pandas as pd
import numpy as np

import prairiedog.config as config
from prairiedog.kmers import Kmers
from prairiedog.gref import GRef
from prairiedog.subgraph_ref import SubgraphRef

log = logging.getLogger("prairiedog")


class GraphRef(GRef):
    """
    Helper class to track node ints, etc.
    """
    def __init__(self, n, output_folder=None, mic_csv=None):
        self.n = n
        self.node_id_count = 0
        if mic_csv:
            self.MIC_DF = pd.read_csv(mic_csv, index_col=0)
        else:
            self.MIC_DF = pd.read_csv(config.MIC_CSV, index_col=0)
        self.MIC_COLUMNS = self.MIC_DF.columns
        # Reference for all files encountered
        self.file_map = {}  # src file str : some int
        self.mic_map = {}  # MIC value : some int
        self.kmer_map = {}  # kmer str : some int
        self.label_map = {}  # some label for a kmer : some int
        # NumPy arrays
        self._node_label_array = None
        self._node_attributes_array = None
        # Output folders
        if output_folder:
            self.output_folder = output_folder
        else:
            pf = '{date:%Y-%m-%d_%H-%M-%S}'.format(
                date=datetime.datetime.now())
            self.output_folder = 'outputs/{}'.format(pf)
        self._setup_folders()
        # Output files
        self.adj_matrix = os.path.join(
            self.output_folder, 'KMERS_A.txt')
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
        self.label_mapping = os.path.join(
            self.output_folder, 'KMERS_label_mapping.txt')

    @property
    def node_label_array(self):
        if self._node_label_array is None:
            log.debug("Initializing node label array to length {}".format(
                self.n))
            self._node_label_array = np.empty(self.n, dtype=int)
        return self._node_label_array

    @property
    def node_attributes_array(self):
        if self._node_attributes_array is None:
            log.debug("Initializing node attribute array to length {}".format(
                self.n))
            self._node_attributes_array = np.empty(self.n, dtype=int)
        return self._node_attributes_array

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
            with open(graph_label_file, 'a') as f:
                f.write('{}\n'.format(self._upsert_map(self.mic_map, mic)))

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
        log.info("Appending {} to KMERS_graph_labels_*.txt and \
            KMERS_graph_indicator.txt".format(km))
        self.node_id_count += km.unique_kmers
        graph_id = self._upsert_map(self.file_map, km.filepath)

        # Call to write out a KMERS_graph_indicator.txt file
        with open(self.graph_indicator, 'a') as f:
            for i in range(km.unique_kmers):
                f.write('{}\n'.format(graph_id))

        # Call to write out KMERS_graph_labels_{}.txt files
        self.write_graph_label(km)

    def _find_kmer_label(self, kmer: str):
        # TODO: implement this
        return "AMR element"

    def record_node_labels(self, pos_id: int, kmer: str):
        """
        Needs to cross-ref kmer against a label of some sort
        :param pos_id:
        :param kmer:
        :return:
        """
        label = self._find_kmer_label(kmer)
        label_id = self._upsert_map(self.label_map, label)
        self.node_label_array[pos_id] = label_id

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
        log.info("Appending subgraph {} to core graph".format(subgraph))
        st = time.time()
        ####
        #   KMERS_A.txt
        ####
        with open(self.adj_matrix, 'a') as f:
            for l in subgraph.graph.edgelist:
                split = l.split(' ')
                assert len(split) == 2
                f.write('{}, {}\n'.format(split[0], split[1]))

        ####
        #   KMERS_graph_indicator.txt
        #   KMERS_graph_labels.txt
        ####
        # So we can map node_id : kmer
        inverted = {
            value: key
            for key, value in subgraph.subgraph_kmer_map.items()}
        assert len(inverted) != 0
        n_nodes = len(subgraph.graph.nodes)
        assert n_nodes != 0
        for node_id in subgraph.graph.nodes:
            kmer = inverted[node_id]
            # Node IDs start at 1, but we are writing to indexes in a NumPy
            # array
            pos_id = node_id - 1
            self.record_node_labels(pos_id, kmer)
            self.record_node_attributes(pos_id, kmer)
        en = time.time()
        log.info("Recorded {} nodes to node arrays in {} s".format(
            n_nodes, en-st))

    def close(self):
        """
        Make sure to write out:
        - all mappings for reference
        - KMERS_node_labels.txt
        - KMERS_node_attributes.txt
        :return:
        """
        # Write out dictionary maps
        def _write_d(fl, di):
            with open(fl, 'a') as f:
                for k, v in di.items():
                    f.write('{}, {}\n'.format(k, v))
        log.info("Writing out file mapping as {}".format(self.file_mapping))
        _write_d(self.file_mapping, self.file_map)
        log.info("Writing out MIC mapping as {}".format(self.mic_mapping))
        _write_d(self.mic_mapping, self.mic_map)
        log.info("Writing out Kmer mapping as {}".format(self.kmer_mapping))
        _write_d(self.kmer_mapping, self.kmer_map)
        log.info("Writing out label mapping as {}".format(self.label_mapping))
        _write_d(self.label_mapping, self.label_map)

        # Write out numpy array
        log.info("Writing out node labels as {}".format(self.node_labels))
        node_label_array_nonzero = self.node_label_array[
            np.nonzero(self.node_label_array)]
        if len(node_label_array_nonzero) == 0:
            log.critical("Length on node label array is zero")
            raise Exception("Length on node label array is zero")
        np.savetxt(
            self.node_labels,
            node_label_array_nonzero,
            fmt='%d')

        log.info("Writing out node attributes as {}".format(
            self.node_attributes))
        node_attributes_array_nonzero = self.node_attributes_array[
            np.nonzero(self.node_attributes_array)]
        if len(node_attributes_array_nonzero) == 0:
            log.critical("Length on node attribute array is zero")
            raise Exception("Length on node attribute array is zero")
        np.savetxt(
            self.node_attributes,
            node_attributes_array_nonzero,
            fmt='%d')
