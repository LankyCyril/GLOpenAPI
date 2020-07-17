#!/usr/bin/env python3
from genefab3 import ColdStorageDataset

DEG_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_differential_expression.csv$'
VIZ_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_output_table.csv$'
PCA_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_PCA_table.csv$'

glds = ColdStorageDataset("GLDS-42")
for assay in glds.assays.values():
    break

for filename, fileinfo in assay.resolve_filename(DEG_CSV_REGEX).items():
    print(filename, fileinfo.timestamp)

df = assay.get_file(DEG_CSV_REGEX).filedata
print(df.columns)
