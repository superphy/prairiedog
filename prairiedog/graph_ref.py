import os
import logging
import itertools
import typing

import pandas as pd
import numpy as np
import torch as th
from sklearn.preprocessing import OneHotEncoder

import prairiedog.config as config
from prairiedog.kmers import Kmers
from prairiedog.gref import GRef

log = logging.getLogger("prairiedog")


class GraphRef(GRef):
    """
    Helper class to track node ints, etc.
    """
    def __init__(self, mic_csv=None):
        self.max_num_nodes = 0
        if mic_csv:
            self.MIC_DF = pd.read_csv(mic_csv, index_col=0)
        else:
            self.MIC_DF = pd.read_csv(config.MIC_CSV, index_col=0)
        self.MIC_COLUMNS = self.MIC_DF.columns

        # Used if you want to reverse the graph label to MIC value
        self.mic_map = {}  # MIC value : some int

        # Used to create unique node labels for later one-hot encoding
        self.kmer_enc, self.num_unique_node_labels = GraphRef._kmer_map()

    @staticmethod
    def _kmer_map() -> typing.Tuple[OneHotEncoder, int]:
        log.debug("Computing Kmer Map...")
        possible_kmers = np.array([
            ''.join(x) for x in itertools.product('ATCG', repeat=config.K)]
        ).reshape(-1, 1)
        # Use Scikit-Learn's OneHotEncoder to create sparse matrices
        enc = OneHotEncoder(handle_unknown='ignore')
        enc.fit_transform(possible_kmers)
        log.debug("Done computing Kmer Map")
        return enc, len(possible_kmers)

    @staticmethod
    def get_short_name(km: Kmers):
        return os.path.basename(km.filepath).split('.')[0]

    def _record_graph_label(self,  km: Kmers):
        """
        Graph labels are the MIC label. This has to be computed for all our
        chosen genomes so everything is indexed before we create graphs.
        :param km:
        :return:
        """
        short_name = GraphRef.get_short_name(km)
        series = self.MIC_DF.loc[short_name, :]
        for label in self.MIC_COLUMNS:
            mic = series[label]
            self._upsert_map(self.mic_map, mic)

    def index_kmers(self, km: Kmers):
        """
        :param km:
        :return:
        """
        log.info("Indexing {} ...".format(km))

        if km.unique_kmers > self.max_num_nodes:
            self.max_num_nodes = km.unique_kmers

        self._record_graph_label(km)

        log.info("Done indexing {}".format(km))

    def get_graph_label(self, km: Kmers, column: str) -> int:
        short_name = GraphRef.get_short_name(km)
        series = self.MIC_DF.loc[short_name, :]
        mic = series[column]
        graph_label = self.mic_map[mic]
        return graph_label

    def get_node_label(self, kmer: str) -> th.Tensor:
        kmer_onehot = self.kmer_enc.transform(
            np.array([kmer]).reshape(-1, 1)
        )
        tensor = th.tensor(kmer_onehot.toarray())
        return tensor
