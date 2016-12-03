from .base import BaseRouter


class PlainRouter(BaseRouter):
    def get_hosts(self, **kwargs):
        return self.hosts


class RandomRouter(BaseRouter):
    def get_hosts(self, **kwargs):
        self.shuffle_hosts()
        return self.hosts
