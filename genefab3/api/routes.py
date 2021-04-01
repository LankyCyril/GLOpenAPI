from genefab3.common.types import Routes
from genefab3.common.exceptions import GeneFabException
from genefab3.api import views


class DefaultRoutes(Routes):
    """Defines standard endpoints"""
 
    def __init__(self, mongo_collections):
        self.mongo_collections = mongo_collections
 
    @Routes.register_endpoint(endpoint="/favicon.<imgtype>", fmt="raw")
    def favicon(self, imgtype): return ""
 
    @Routes.register_endpoint(endpoint="/debug/", fmt="raw")
    def debug(self): return "OK"
 
    @Routes.register_endpoint(endpoint="/debug/error/", fmt="raw")
    def debug_error(self): raise GeneFabException("Generic error", debug=True)
 
    @Routes.register_endpoint(endpoint="/", fmt="html")
    def root(self): return "Hello space"
 
    @Routes.register_endpoint()
    def status(self): return views.status.get(self.mongo_collections)
