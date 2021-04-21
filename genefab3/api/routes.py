from genefab3.common.types import Routes, AnnotationDataFrame, DataDataFrame
from genefab3.api import views
from pandas import DataFrame
from typing import Union
from flask import Response


class DefaultRoutes(Routes):
    """Defines standard endpoints"""
 
    @Routes.register_endpoint("/favicon.<imgtype>")
    def favicon(self, imgtype, context=None) -> bytes:
        return b''
 
    @Routes.register_endpoint("/")
    def root(self, context) -> str:
        return views.root.get(self.mongo_collections, context=context)
 
    @Routes.register_endpoint()
    def status(self, context=None) -> DataFrame:
        return views.status.get(self.mongo_collections, context=context)
 
    @Routes.register_endpoint()
    def assays(self, context) -> AnnotationDataFrame:
        return views.metadata.get(
            self.mongo_collections, locale=self.locale, context=context,
            id_fields=["accession", "assay"], aggregate=True,
        )
 
    @Routes.register_endpoint()
    def samples(self, context) -> AnnotationDataFrame:
        return views.metadata.get(
            self.mongo_collections, locale=self.locale, context=context,
            id_fields=["accession", "assay", "sample name"], aggregate=False,
        )
 
    @Routes.register_endpoint()
    def data(self, context) -> Union[DataDataFrame, Response]:
        return views.data.get(
            self.mongo_collections, locale=self.locale, context=context,
            sqlite_dbs=self.sqlite_dbs, adapter=self.adapter,
        )
