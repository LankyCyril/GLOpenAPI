from genefab3.common.types import Routes
from genefab3.api import views


class DefaultRoutes(Routes):
    """Defines standard endpoints"""
 
    @Routes.register_endpoint("/favicon.<imgtype>")
    def favicon(self, imgtype, context=None):
        return b''
 
    @Routes.register_endpoint("/js/<filename>")
    def js(self, filename, context=None):
        return views.static.get(
            directory="js", filename=filename,
            mimetype="application/javascript",
        )
 
    @Routes.register_endpoint("/")
    def root(self, context):
        return views.root.get(
            mongo_collections=self.genefab3_client.mongo_collections,
            context=context,
        )
 
    @Routes.register_endpoint()
    def status(self, context=None):
        return views.status.get(
            genefab3_client=self.genefab3_client,
            sqlite_dbs=self.genefab3_client.sqlite_dbs, context=context,
        )
 
    @Routes.register_endpoint()
    def assays(self, context):
        return views.metadata.get(
            mongo_collections=self.genefab3_client.mongo_collections,
            id_fields=["accession", "assay name"], condensed=1,
            locale=self.genefab3_client.locale, context=context,
        )
 
    @Routes.register_endpoint()
    def samples(self, context):
        return views.metadata.get(
            mongo_collections=self.genefab3_client.mongo_collections,
            id_fields=["accession", "assay name", "sample name"], condensed=0,
            locale=self.genefab3_client.locale, context=context,
        )
 
    @Routes.register_endpoint()
    def data(self, context):
        return views.data.get(
            mongo_collections=self.genefab3_client.mongo_collections,
            sqlite_dbs=self.genefab3_client.sqlite_dbs,
            adapter=self.genefab3_client.adapter,
            locale=self.genefab3_client.locale, context=context,
        )
