from genefab3.common.types import Routes
from genefab3.api import views


class DefaultRoutes(Routes):
    """Defines standard endpoints"""
 
    def __init__(self, mongo_collections, locale):
        self.mongo_collections, self.locale = mongo_collections, locale
 
    @Routes.register_endpoint(endpoint="/favicon.<imgtype>", fmt="raw")
    def favicon(self, imgtype, context=None):
        return ""
 
    @Routes.register_endpoint(endpoint="/", fmt="html", cache=False)
    def root(self, context):
        return views.root.get(self.mongo_collections, context=context)
 
    @Routes.register_endpoint()
    def status(self, context=None):
        return views.status.get(self.mongo_collections)
 
    @Routes.register_endpoint()
    def assays(self, context):
        return views.metadata.get(
            self.mongo_collections, locale=self.locale, context=context,
            include=(), aggregate=True,
        )
 
    @Routes.register_endpoint()
    def samples(self, context):
        return views.metadata.get(
            self.mongo_collections, locale=self.locale, context=context,
            include={"info.sample name"}, aggregate=False,
        )
 
    @Routes.register_endpoint(fmt="raw")
    def file(self, context):
        return views.file.get(
            self.mongo_collections, locale=self.locale, context=context,
        )
 
    @Routes.register_endpoint()
    def data(self, context):
        return views.data.get(
            self.mongo_collections, locale=self.locale, context=context,
        )
