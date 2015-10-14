import logging
from .transportbase import TransportBase
from ..exceptions import SolrError
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
            logging.error("Requests Module not found. Please install it before using this transport")
            raise ImportException("Requests Module not found. Please install it before using this transport")
        self.session = requests.session()
        if self.auth:
            self.session.auth = (self.auth[0],self.auth[1])
        
    
    
    
    def _send(self,host,method='GET',**kwargs):
        url = None
        params = kwargs['params'] if 'params' in kwargs else {}
        if 'wt' not in params:
            params['wt'] = 'json'
        params['indent'] = False
        for field in params: 
            if type(params[field]) is bool:
                params[field] = str(params[field]).lower()
                
        if not host.endswith('/'):
            host += '/'
            
        data = kwargs['data'] if 'data' in kwargs else {}
        if kwargs['endpoint']:
            if 'collection' in kwargs:
                url = "{}{}/{}".format(host,kwargs['collection'],kwargs['endpoint'])
            else:
                url = host + kwargs['endpoint']
        else:
            raise ValueError("No URL 'endpoint' set in parameters to send_request")
        
        self.logger.debug("Sending Request to {} with {}".format(url,", ".join([str("{}={}".format(key, params[key])) for key in params])))
        
        try:
            res = self.session.request(method, url, params=params, data=data,headers = {'content-type': 'application/json'})
            if res.status_code == 404:
                raise ConnectionError("404 - {}".format(res.url))
            if res.status_code == 401:
                raise ConnectionError("401 - {}".format(res.url))
            elif res.status_code == 500:
                raise SolrError("500 - " + res.url + " "+res.text)
            elif res.status_code == 400:
                raise SolrError(res.url+" "+res.text)
            return res.json()
        except requests.exceptions.ConnectionError as e:
            self.logger.exception(e)
            raise ConnectionError(e)
        except ConnectionError as e:
            self.logger.exception(e)
            raise ConnectionError(e)
            
        