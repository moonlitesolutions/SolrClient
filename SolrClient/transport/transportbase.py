import logging
from ..exceptions import *
from ..routers.plain import PlainRouter


class TransportBase():
    """
    Base Transport Class
    """

    #def __init__(self, solr, auth=(None, None), devel=None, host=None, router=PlainRouter, **kwargs):
    def __init__(self, solr, auth=(None, None), devel=None, host=None, **kwargs):
        self.logger = logging.getLogger(str(__package__))
        self.auth = auth
        self.host = host if type(host) is list else [host]
        self._devel = devel
        self._action_log = []
        self._action_log_count = 1000
        self.solr = solr
        #self.router = router(self, host, **kwargs)
        self.setup()

    def _add_to_action(self, action):
        self._action_log.append(action)
        if len(self._action_log) >= self._action_log_count:
            self._action_log.pop(0)

    def _retry(function):
        """
        Internal mechanism to try to send data to multiple Solr Hosts if
        the query fails on the first one.
        """

        def inner(self, **kwargs):
            last_exception = None
            #for host in self.router.get_hosts(**kwargs):
            for host in self.host:
                try:
                    return function(self, host, **kwargs)
                except SolrError as e:
                    self.logger.exception(e)
                    raise
                except ConnectionError as e:
                    self.logger.exception("Tried connecting to Solr, but couldn't because of the following exception.")
                    if '401' in e.__str__():
                        raise
                    last_exception = e
            # raise the last exception after contacting all hosts instead of returning None
            if last_exception is not None:
                raise last_exception
        return inner

    @_retry
    def send_request(self, host, **kwargs):
        if self._devel:
            self._add_to_action({'host': host, 'params': dict(**kwargs)})
        res_dict, c_inf = self._send(host, **kwargs)
        if 'errors' in res_dict:
            error = ", ".join([x for x in res_dict['errors'][0]['errorMessages']])
            raise SolrError(error)
        elif 'error' in res_dict:
            raise SolrError(str(res_dict['error']))
        return [res_dict, c_inf]

    def _log_connection_error(self, method, full_url, body, duration, status_code=None, exception=None):
        self.logger.warning("Connection Error: [{}] {} - {} - {}".format(status_code, method, full_url, body))
