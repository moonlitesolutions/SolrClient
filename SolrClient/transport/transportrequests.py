import time
from .transportbase import TransportBase
from ..exceptions import SolrError, ConnectionError

try:
    import requests
    req = True
except ImportError:
    req = False



class TransportRequests(TransportBase):
    """
    Class that Uses Requests as Transport Mechanism.
    """

    def setup(self):
        if not req:
            raise ImportError("Requests Module not found. Please install it before using this transport")
        self.session = requests.session()
        if self.auth and self.auth != (None, None):
            self.session.auth = (self.auth[0], self.auth[1])

    def _send(self, host, method='GET', endpoint=None, collection=None, params=None, headers=None, data=None, **kwargs):
        if endpoint is None:
            raise ValueError("No URL 'endpoint' set in parameters to send_request")
        if params is None:
            params = {}
        # put each kwarg into the params, like min_rf, _route_ etc
        params.update(wt='json', indent=False, **kwargs)
        if not host.endswith('/'):
            host += '/'
        for field in params:
            if type(params[field]) is bool:
                params[field] = str(params[field]).lower()
        if collection is not None:
            url = "{}{}/{}".format(host, collection, endpoint)
        else:
            url = host + endpoint
        if headers is None:
            headers = {'content-type': 'application/json'}

        self.logger.debug("Sending Request to {} with {}".format(url, ", ".join(
            (str("{}={}".format(key, params[key])) for key in params))))

        # Some code used from ES python client.
        start = time.time()
        try:
            res = self.session.request(method, url, params=params, data=data, headers=headers)
            duration = time.time() - start
            self.logger.debug("Request Completed in {} Seconds".format(round(duration, 2)))
        except requests.exceptions.SSLError as e:
            self._log_connection_error(method, url, data, time.time() - start, exception=e)
            raise ConnectionError('N/A', str(e), e)
        except requests.Timeout as e:
            self._log_connection_error(method, url, data, time.time() - start, exception=e)
            raise ConnectionError('TIMEOUT', str(e), e)
        except requests.ConnectionError as e:
            self._log_connection_error(method, url, str(e), time.time() - start, exception=e)
            raise ConnectionError('N/A', str(e), e)

        if 200 <= res.status_code < 300:
            return [res.json(), {'url': res.url}]
        if res.status_code == 404:
            raise ConnectionError("404 - {}".format(res.url))
        elif res.status_code == 401:
            raise ConnectionError("401 - {}".format(res.url))
        elif res.status_code == 500:
            raise SolrError("500 - " + res.url + " " + res.text)
        else:
            raise SolrError(res.url + " " + res.text)
