import json
import logging
from .transport import TransportRequests
from .schema import Schema
from .exceptions import *
from .solrresp import SolrResponse

class Collections():
    '''
    Provides an interface to Solr Collections API.
    '''
    
    def __init__(self, solr, log):
        self.solr = solr
        self.logger = log
    
    def api(self, action, args={}):
        '''
        Sends a request to Solr Collections API. 
        Documentation is here: https://cwiki.apache.org/confluence/display/solr/Collections+API
        
        :param string action: Name of the collection for the action
        :param string args: Dictionary of specific parameters for action
        '''
        args['action'] = action.upper()
        res, con_info = self.solr.transport.send_request(endpoint='admin/collections', params=args)
        if 'responseHeader' in res and res['responseHeader']['status'] == 0:
            return res, con_info
        else:
            raise SolrError("Error Issuing Collections API Call for: {} +".format(con_info, res))