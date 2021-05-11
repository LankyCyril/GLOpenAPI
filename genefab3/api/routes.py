from genefab3.common.types import Routes, StringIterator
from genefab3.api import views
from genefab3.common.types import StreamedAnnotationTable, StreamedDataTable
from typing import Union
from flask import Response


class DefaultRoutes(Routes):
    """Defines standard endpoints"""
 
    @Routes.register_endpoint("/favicon.<imgtype>")
    def favicon(self, imgtype, context=None) -> bytes:
        return b''
 
    @Routes.register_endpoint("/")
    def root(self, context) -> StringIterator:
        return views.root.get(
            mongo_collections=self.genefab3_client.mongo_collections,
            context=context,
        )
 
    @Routes.register_endpoint()
    def status(self, context=None) -> StreamedAnnotationTable:
        return views.status.get(
            genefab3_client=self.genefab3_client,
            sqlite_dbs=self.genefab3_client.sqlite_dbs, context=context,
        )
 
    @Routes.register_endpoint()
    def assays(self, context) -> StreamedAnnotationTable:
        return views.metadata.get(
            mongo_collections=self.genefab3_client.mongo_collections,
            id_fields=["accession", "assay name"], condensed=1,
            locale=self.genefab3_client.locale, context=context,
        )
 
    @Routes.register_endpoint()
    def samples(self, context) -> StreamedAnnotationTable:
        return views.metadata.get(
            mongo_collections=self.genefab3_client.mongo_collections,
            id_fields=["accession", "assay name", "sample name"], condensed=0,
            locale=self.genefab3_client.locale, context=context,
        )
 
    @Routes.register_endpoint()
    def data(self, context) -> Union[StreamedDataTable, Response]:
        return views.data.get(
            mongo_collections=self.genefab3_client.mongo_collections,
            sqlite_dbs=self.genefab3_client.sqlite_dbs,
            adapter=self.genefab3_client.adapter,
            locale=self.genefab3_client.locale, context=context,
        )
