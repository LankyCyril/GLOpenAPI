#!/usr/bin/env python3
from genefab3.coldstorage import ColdStorageDataset

glds = ColdStorageDataset("GLDS-42")
print(glds.filedates)
