from genefab3.common.types import Routes
from genefab3.common.exceptions import GeneFabException
from genefab3.api import views


register_endpoint = Routes.register_endpoint


class DefaultRoutes(Routes):
 
    def __init__(self, mongo_collections):
        self.mongo_collections = mongo_collections
 
    @register_endpoint(endpoint="/favicon.<imgtype>", fmt="raw")
    def favicon(self, imgtype):
        return ""
 
    @register_endpoint(endpoint="/debug/", fmt="raw")
    def debug(self):
        return "OK"
 
    @register_endpoint(endpoint="/debug/error/", fmt="raw")
    def debug_error(self):
        raise GeneFabException("Generic error test")
        return "OK (raised exception)"
 
    @register_endpoint(endpoint="/", fmt="html")
    def root(self):
        return "Hello space"
 
    @register_endpoint()
    def status(self):
        return views.status.get(self.mongo_collections)
