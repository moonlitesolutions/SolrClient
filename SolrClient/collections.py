import json
import logging
from .transport import TransportRequests
from .schema import Schema
from .exceptions import *
from .solrresp import SolrResponse


class Collections():
    """
    Provides an interface to Solr Collections API.
    """

    def __init__(self, solr, log):

        self.solr = solr
        self.logger = log
        self.solr_clients = {}

    def api(self, action, args=None):
        """
        Sends a request to Solr Collections API.
        Documentation is here: https://cwiki.apache.org/confluence/display/solr/Collections+API

        :param string action: Name of the collection for the action
        :param dict args: Dictionary of specific parameters for action
        """
        if args is None:
            args = {}
        args['action'] = action.upper()

        try:
            res, con_info = self.solr.transport.send_request(endpoint='admin/collections', params=args)
        except Exception as e:
            self.logger.error("Error querying SolrCloud Collections API. ")
            self.logger.exception(e)
            raise e

        if 'responseHeader' in res and res['responseHeader']['status'] == 0:
            return res, con_info
        else:
            raise SolrError("Error Issuing Collections API Call for: {} +".format(con_info, res))

    def clusterstatus(self):
        """
        Returns a slightly slimmed down version of the clusterstatus api command. It also gets count of documents in each shard on each replica and returns
        it as doc_count key for each replica.

        """

        res = self.cluster_status_raw()

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
                            out[collection][shard][replica]['doc_count'] = False
                        else:
                            out[collection][shard][replica]['doc_count'] = self._get_collection_counts(
                                out[collection][shard][replica])
        except Exception as e:
            self.logger.error("Couldn't parse response from clusterstatus API call")
            self.logger.exception(e)

        return out

    def cluster_status_raw(self, **kwargs):
        """
        Returns raw output of the clusterstatus api command.

        """
        res, con_info = self.api('clusterstatus', **kwargs)
        return res

    def exists(self, collection):
        """
        Return True if a collection exists.
        """
        all_collections = self.list()
        if collection in all_collections:
            return True

    def list(self):
        """
        Returns a list[string] of all collection names on the cluster.
        """
        res, info = self.api('LIST')
        collections = res['collections']
        return collections

    def create(self, name, numShards, params=None):
        """
        Create a new collection.
        """
        if params is None:
            params = {}
        params.update(
            name=name,
            numShards=numShards
        )
        return self.api('CREATE', params)

    def _get_collection_counts(self, core_data):
        """
        Queries each core to get individual counts for each core for each shard.
        """
        if core_data['base_url'] not in self.solr_clients:
            from SolrClient import SolrClient
            self.solr_clients['base_url'] = SolrClient(core_data['base_url'], log=self.logger)
        try:
            return self.solr_clients['base_url'].query(core_data['core'],
                                                       {'q': '*:*',
                                                        'rows': 0,
                                                        'distrib': 'false',
                                                        }).get_num_found()
        except Exception as e:
            self.logger.error("Couldn't get Counts for {}/{}".format(core_data['base_url'], core_data['core']))
            self.logger.exception(e)
            return False

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

    def check_status(self, ignore=(), status=None):
        """
        Checks status of each collection and shard to make sure that:
          a) Cluster state is active
          b) Number of docs matches across replicas for a given shard.
        Returns a dict of results for custom alerting.
        """
        self.SHARD_CHECKS = [
            {'check_msg': 'Bad Core Count Check', 'f': self._check_shard_count},
            {'check_msg': 'Bad Shard Cluster Status', 'f': self._check_shard_status}
        ]
        if status is None:
            status = self.clusterstatus()
        out = {}
        for collection in status:
            out[collection] = {}
            out[collection]['coll_status'] = True  # Means it's fine
            out[collection]['coll_messages'] = []
            for shard in status[collection]:
                self.logger.debug("Checking {}/{}".format(collection, shard))
                s_dict = status[collection][shard]
                for check in self.SHARD_CHECKS:
                    if check['check_msg'] in ignore:
                        continue
                    res = check['f'](s_dict)
                    if not res:
                        out[collection]['coll_status'] = False
                        if check['check_msg'] not in out[collection]['coll_messages']:
                            out[collection]['coll_messages'].append(check['check_msg'])
                        self.logger.debug(s_dict)
        return out

    def _check_shard_count(self, cores_dict):
        if len(set([cores_dict[core]['doc_count'] for core in cores_dict])) > 1:
            return False
        return True

    def _check_shard_status(self, cores_dict):
        for core in cores_dict:
            if cores_dict[core]['state'] != 'active':
                return False
        return True
