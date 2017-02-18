from .base import BaseRouter


class PlainRouter(BaseRouter):
    def get_hosts(self, **kwargs):
        return self.hosts


class ShuffleRouter(BaseRouter):
    def __init__(self, solr, hosts):
        super().__init__(solr, hosts)
        # only shuffle the hosts once, so they're in a different order in all your processes
        # not all proesses contact the same host, but distribute requests
        self.shuffle_hosts()


class RandomRouter(BaseRouter):
    def get_hosts(self, **kwargs):
        self.shuffle_hosts()
        return self.hosts
