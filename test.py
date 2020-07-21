#!/usr/bin/env python3
from genefab3 import ColdStorageDataset
from genefab3.coldstoragedataset import get_json
from pandas import DataFrame

DEG_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_differential_expression.csv$'
VIZ_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_output_table.csv$'
PCA_CSV_REGEX = r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_PCA_table.csv$'

glds_json = get_json("GLDS-239", "glds")
glds = ColdStorageDataset("GLDS-239", glds_json)
for assay in glds.assays.values():
    break

print(assay.factors.minimal)
pca = assay.get_file(PCA_CSV_REGEX, astype=DataFrame, sep=",").filedata
print(pca.iloc[:, :6])
