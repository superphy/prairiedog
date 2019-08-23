from prairiedog.subgraph_ref import uncouple_edge_type


class Hit:
    def __init__(self, string: str, edge_type: str):
        self.sequence = string
        self.source = edge_type
        sample, contig = uncouple_edge_type(edge_type)
        self.sample = sample
        self.contig = contig


class PrettyHits:
    @staticmethod
    def parse_hits(hits: list) -> dict:
        # Create structure:
        # {
        #     "sample": {
        #         "contig": {"variantA", "variantB"}
        #     }
        # }
        sample_map = {}
        for hit in hits:
            sample = hit.sample
            if sample not in sample_map:
                sample_map[sample] = {}

            contig_map = sample_map[sample]
            contig = hit.contig
            if contig not in contig_map:
                contig_map[contig] = set()
            variant_set = contig_map[contig]
            variant_set.add(hit.sequence)
        return sample_map

    def __init__(self, list_hits: list):
        hits = [
            Hit(string=hit['string'], edge_type=hit['edge_type'])
            for hit in list_hits
        ]
        self.sample_map = PrettyHits.parse_hits(hits)

    def __str__(self):
        s = "\n"
        for sample, contig_map in self.sample_map.items():
            s += "Sample {} has:\n".format(sample)
            for contig, variant_set in contig_map.items():
                s += "Contig {} with variant(s):\n".format(contig)
                for variant in variant_set:
                    s += "{}\n".format(variant)
        return s
