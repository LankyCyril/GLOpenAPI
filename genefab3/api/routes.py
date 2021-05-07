from genefab3.common.types import Routes, StreamedAnnotationTable, DataDataFrame
from genefab3.db.sql.files import CachedBinaryFile
from genefab3.api import views
from pandas import DataFrame
from typing import Union
from flask import Response


class DefaultRoutes(Routes):
    """Defines standard endpoints"""
 
    @Routes.register_endpoint("/favicon.<imgtype>")
    def favicon(self, imgtype, context=None) -> bytes:
        if self.genefab3_client.adapter.get_favicon_urls():
            ico_file = CachedBinaryFile(
                name="favicon.ico", identifier="RESOURCE/favicon.ico",
                sqlite_db=self.genefab3_client.sqlite_dbs.blobs["db"],
                urls=self.genefab3_client.adapter.get_favicon_urls(),
                timestamp=0,
            )
            return ico_file.data
        else:
            return b''
 
    @Routes.register_endpoint("/")
    def root(self, context) -> str:
        return views.root.get(
            mongo_collections=self.genefab3_client.mongo_collections,
            context=context,
        )
 
    @Routes.register_endpoint()
    def status(self, context=None) -> DataFrame:
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
    def data(self, context) -> Union[DataDataFrame, Response]:
        return views.data.get(
            mongo_collections=self.genefab3_client.mongo_collections,
            sqlite_dbs=self.genefab3_client.sqlite_dbs,
            adapter=self.genefab3_client.adapter,
            locale=self.genefab3_client.locale, context=context,
        )
