from genefab3.common.types import Routes
from genefab3.api import views


class DefaultRoutes(Routes):
    """Defines standard endpoints"""
 
    @Routes.register_endpoint("/version/")
    def version(self, context=None):
        return str(getattr(self.genefab3_client, "app_version", ""))
 
    @Routes.register_endpoint("/favicon.<imgtype>")
    def favicon(self, imgtype, context=None):
        return b''
 
    @Routes.register_endpoint("/libs/js/<filename>")
    def js(self, filename, context=None):
        return views.static.get(
            directory="libs/js", filename=filename,
            mode="rt", mimetype="application/javascript",
        )
 
    @Routes.register_endpoint("/libs/css/<filename>")
    def css(self, filename, context=None):
        return views.static.get(
            directory="libs/css", filename=filename,
            mode="rt", mimetype="text/css",
        )
 
    @Routes.register_endpoint("/libs/css/images/<filename>")
    def css_image(self, filename, context=None):
        return views.static.get(
            directory="libs/css/images", filename=filename,
            mode="rb", mimetype="image/gif",
        )
 
    @Routes.register_endpoint("/")
    def root(self, context):
        return views.root.get(
            genefab3_client=self.genefab3_client,
            mongo_collections=self.genefab3_client.mongo_collections,
            context=context,
        )
 
    @Routes.register_endpoint("/images/<filename>")
    def image(self, filename, context=None):
        return views.static.get(
            directory="images", filename=filename,
            mode="rb", mimetype="image/png",
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
