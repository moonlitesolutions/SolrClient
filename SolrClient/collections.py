import json
import logging
from .transport import TransportRequests
from .schema import Schema
from .exceptions import *
from .solrresp import SolrResponse
from collections import defaultdict

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
    
    def clusterstatus(self):
        '''
        Returns a slightly slimmed down version of the clusterstatus api command. 
        '''
        res, con_info =  self.api('clusterstatus')
        cluster = res['cluster']['collections']
        out = {}
        try:
            for collection in cluster:
                out[collection] = {}
                for shard in cluster[collection]['shards']:
                    out[collection][shard] = {}
                    for replica in cluster[collection]['shards'][shard]['replicas']:
                        out[collection][shard][replica] = cluster[collection]['shards'][shard]['replicas'][replica]
                        if out[collection][shard][replica]['state'] != 'active':
                            self.logger.error("{}->{}->{} is not Active".format(collection, shard, replica))
        except Exception as e:
            self.logger.error("Couldn't parse response from clusterstatus API call")
            self.logger.exception(e)
        return out
    

    def _for_core(self, cluster_resp=None):
        if cluster_resp is None:
            cluster_resp = self.clusterstatus()
        for collection in cluster_resp:
            for shard in cluster_resp[collection]:
                for core in cluster_resp[collection][shard]:
                    yield collection, shard, core, cluster_resp[collection][shard][core]

    def _for_shard(self, cluster_resp=None):
        if cluster_resp is None:
            cluster_resp = self.clusterstatus()
        for collection in cluster_resp:
            for shard in cluster_resp[collection]:
                yield collection, shard, cluster_resp[collection][shard]
                    
    def _check_collection(self, collection, cluster_resp=None ):
        for coll, shard, core, c_data in self._for_core(cluster_resp):
            if collection == coll:
                if c_data['state'] != 'active':
                    return False
                return True
        self.logger.error("Couldn't find Collection {} in cluster_resp".format(collection))
        
    def get_collection_counts(self, cluster_resp=None):
        '''
        Queries each core to get individual counts for each core for each shard. 
        '''
        from SolrClient import SolrClient
        temp = {}
        for coll, shard, core, c_data in self._for_core(cluster_resp):
            if coll not in temp:
                temp[coll] = {}
            if shard not in temp[coll]:
                temp[coll][shard] = {}
            ts = SolrClient( c_data['base_url'] )
            temp[coll][shard][c_data['core']] = ts.query(c_data['core'], 
                                                        {'q':'*:*',
                                                        'rows':0,
                                                        'distrib': 'false',
                                                        }).get_num_found()
        return temp
        
    def check_collection_counts(self, counts=None, cb=None):
        if counts is None:
            counts = self.get_collection_counts()
        for collection, shard, s_data in self._for_shard(counts):
            for replica in s_data:
                self.logger.info("Item Count for {}->{}->{}: {}".format(collection, shard, replica, s_data[replica]))
            if len(set(s_data.values())) > 1:
                self.logger.error("Count Mismatch for {}->{}->{} {}".format( collection, shard, replica, "/".join([str(x) for x in list(s_data.values())])))
                if cb:
                    if hasattr(cb, '__call__'):
                        self.logger.debug("Calling callback on collection count mismatch")
                        cb(s_data)
                    else:
                        raise TypeError("Callback passed to check_collection_counts is not callable")
        return counts
        
    def check_collections(self, collection=None):
        cluster = self.clusterstatus()
        out = {}
        for coll in cluster.keys():
            out[coll] = self._check_collection(coll, cluster)
            self.logger.info("Collection {} is {}".format(coll, "Active" if out[coll] else "having issues"))
        return out