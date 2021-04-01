from genefab3.common.types import Routes
from genefab3.common.exceptions import GeneFabException
from genefab3.api import views
from genefab3.api.parser import Context


class DefaultRoutes(Routes):
    """Defines standard endpoints"""
 
    def __init__(self, mongo_collections, locale):
        self.mongo_collections, self.locale = mongo_collections, locale
 
    @Routes.register_endpoint(endpoint="/favicon.<imgtype>", fmt="raw")
    def favicon(self, imgtype):
        return ""
 
    @Routes.register_endpoint(endpoint="/debug/", fmt="raw")
    def debug(self):
        return "OK"
 
    @Routes.register_endpoint(endpoint="/debug/error/", fmt="raw")
    def debug_error(self):
        raise GeneFabException("Generic error", test=None)
 
    @Routes.register_endpoint(endpoint="/", fmt="html")
    def root(self):
        return "Hello space"
 
    @Routes.register_endpoint()
    def status(self):
        return views.status.get(self.mongo_collections)
 
    @Routes.register_endpoint()
    def assays(self):
        return views.metadata.get(
            self.mongo_collections, locale=self.locale, context=Context(),
            include=(), aggregate=True,
        )
 
    @Routes.register_endpoint()
    def samples(self):
        return views.metadata.get(
            self.mongo_collections, locale=self.locale, context=Context(),
            include={"info.sample name"}, aggregate=False,
        )
