from controller.handler import SearchAPI
#from controller.handler import LyricsAPI


def initialize_endpoints(api):
    api.add_resource(SearchAPI, "/search")
