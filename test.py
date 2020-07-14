#!/usr/bin/env python3
from genefab3 import ColdStorageDataset

glds = ColdStorageDataset("GLDS-42")
for assay in glds.assays.values():
    break
print(assay.metadata.dataframe["Protocol REF"])
