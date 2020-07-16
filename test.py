#!/usr/bin/env python3
from genefab3 import ColdStorageDataset

glds = ColdStorageDataset("GLDS-42")
for assay in glds.assays.values():
    break

print(glds.resolve_filename(r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_differential_expression.csv$'))
