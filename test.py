#!/usr/bin/env python3
from genefab3 import ColdStorageDataset

DEG_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_differential_expression.csv$'
VIZ_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_output_table.csv$'
PCA_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_PCA_table.csv$'

glds = ColdStorageDataset("GLDS-239")
for assay in glds.assays.values():
    break

print(assay.annotation.minimal)
print(assay.get_file(PCA_CSV_REGEX).filedata.iloc[:3, :4])
