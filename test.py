#!/usr/bin/env python3
from genefab3 import ColdStorageDataset

glds = ColdStorageDataset("GLDS-42")
for assay in glds.assays.values():
    break

for filename, fileinfo in assay.resolve_filename(r'CEL', r'.*FLT.*').items():
    print(filename, fileinfo.timestamp)

print(assay.annotation.tidy)
print(assay.factors)
