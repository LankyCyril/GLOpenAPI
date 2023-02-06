from glopenapi.api.types import Routes
from glopenapi.api import views


class DefaultRoutes(Routes):
    """Defines standard endpoints"""
 
    @Routes.register_endpoint("/")
    def root(self, context):
        return views.root.get(
            glopenapi_client=self.glopenapi_client,
            mongo_collections=self.glopenapi_client.mongo_collections,
            context=context,
        )
 
    @Routes.register_endpoint("/root.js")
    def root_js(self, context):
        # TODO/DRY: this is not very clean; similar logic should be applied to
        # javascript generated for &format=browser; therefore, these logics
        # should be unified
        return views.root.get(
            glopenapi_client=self.glopenapi_client,
            mongo_collections=self.glopenapi_client.mongo_collections,
            context=context,
        )
 
    @Routes.register_endpoint()
    def status(self, context=None):
        return views.status.get(
            glopenapi_client=self.glopenapi_client,
            sqlite_dbs=self.glopenapi_client.sqlite_dbs, context=context,
        )
 
    @Routes.register_endpoint()
    def assays(self, context):
        return views.metadata.get(
            mongo_collections=self.glopenapi_client.mongo_collections,
            id_fields=["accession", "assay name"],
            context=context, condensed=True, unique_counts=False,
            locale=self.glopenapi_client.locale,
        )
 
    @Routes.register_endpoint()
    def samples(self, context):
        return views.metadata.get(
            mongo_collections=self.glopenapi_client.mongo_collections,
            id_fields=["accession", "assay name", "sample name"],
            context=context, condensed=False, unique_counts=False,
            locale=self.glopenapi_client.locale,
        )
 
    @Routes.register_endpoint()
    def metadata(self, context):
        return self.samples(context)
 
    @Routes.register_endpoint("/metadata-counts/")
    def metadata_counts(self, context):
        return views.metadata.get(
            mongo_collections=self.glopenapi_client.mongo_collections,
            id_fields=["accession", "assay name", "sample name"],
            context=context, condensed=False, unique_counts=True,
            locale=self.glopenapi_client.locale,
        )
 
    @Routes.register_endpoint()
    def data(self, context):
        return views.data.get(
            mongo_collections=self.glopenapi_client.mongo_collections,
            sqlite_dbs=self.glopenapi_client.sqlite_dbs,
            adapter=self.glopenapi_client.adapter,
            locale=self.glopenapi_client.locale, context=context,
        )
 
    @Routes.register_endpoint("/favicon.<imgtype>")
    def favicon(self, imgtype, context=None):
        return b''
 
    @Routes.register_endpoint("/css/<filename>")
    def css(self, filename, context=None):
        return views.static.get(
            directory="css", filename=filename, mode="rt", mimetype="text/css",
        )
 
    @Routes.register_endpoint("/images/<filename>")
    def images(self, filename, context=None):
        return views.static.get(
            directory="images", filename=filename, mode="rb",
            mimetype="image/"+(filename.endswith("png") and "png" or "svg+xml"),
        )
 
    @Routes.register_endpoint("/libs/js/<filename>")
    def libs_js(self, filename, context=None):
        return views.static.get(
            directory="libs/js", filename=filename,
            mode="rt", mimetype="application/javascript",
        )
 
    @Routes.register_endpoint("/libs/css/<filename>")
    def libs_css(self, filename, context=None):
        return views.static.get(
            directory="libs/css", filename=filename,
            mode="rt", mimetype="text/css",
        )
 
    @Routes.register_endpoint("/libs/css/images/<filename>")
    def libs_css_images(self, filename, context=None):
        return views.static.get(
            directory="libs/css/images", filename=filename,
            mode="rb", mimetype="image/gif",
        )
 
    @Routes.register_endpoint("/version/")
    def version(self, context=None):
        return str(getattr(self.glopenapi_client, "app_version", ""))
 
    @Routes.register_endpoint("/changelog/")
    def changelog(self, context=None):
        return views.static.get(
            directory=".", filename="changelog.md",
            mode="rt", mimetype="text/plain",
        )
