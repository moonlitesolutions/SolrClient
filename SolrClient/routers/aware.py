import random
import sys
from .base import BaseRouter
from bisect import bisect_left
from datetime import datetime

try:
    import mmh3
except ImportError:
    try:
        from . import pymmh3 as mmh3
    except:
        raise ImportError("You need https://pypi.python.org/pypi/mmh3 to use this, friend.")

# always prefer leader in these endpoints since even solr will route to leader itself
endpoints_prefer_leader = {'update', 'update/json'}


class AwareRouter(BaseRouter):
    """
    You want this only when you have collections
    Requires _route_ to be set.
    Optional prefer_leader=True when doing queries.
    """
    shard_map = None
    last_refresh = datetime(1990, 1, 1)
    refresh_ttl = 300

    def __init__(self, solr, hosts, refresh_map_every=300, **kwargs):
        super().__init__(solr, hosts, **kwargs)
        self.refresh_ttl = refresh_map_every
        self.shuffle_hosts()

    def get_hosts(self, collection=None, endpoint=None, _route_=None, **kwargs):
        # pop it from kwargs so it doesn't get passed to transport._send(kwargs)
        prefer_leader = kwargs.pop('prefer_leader', False)
        if _route_ is not None and collection is not None and collection:
            shard_map = self.get_shard_map()
            if collection in shard_map:
                # do we have multiple _route_ keys, chose a random one
                hash_keys = _route_.split(',')
                if len(hash_keys) > 1:  # chose random key
                    route_key = random.choice(hash_keys)
                else:
                    route_key = hash_keys[0]
                hash_int = mmh3.hash(route_key)
                hash_key = format(hash_int, 'x')  # in hex
                collection_map = shard_map[collection]
                slots = collection_map['slots']
                key_index = bisect_left(slots, hash_key)
                nearest_hash = slots[key_index]
                shard_name = collection_map['hash_to_shard'][nearest_hash]
                replicas = collection_map['shards'][shard_name]
                self.logger.debug('routing-result: key:%s index:%s near:%s shard:%s replicas:%s slots:%s' % (
                    hash_key, key_index, nearest_hash, shard_name, replicas, slots))
                if not prefer_leader and endpoint not in endpoints_prefer_leader:
                    # just shuffle the replicas so we contact a random one
                    if len(replicas) > 1:
                        replicas = list(replicas)
                        random.shuffle(replicas)
                        replicas = tuple(replicas)
                # depending how fresh the shard-map is, nodes may have gone down and replicas moved elsewhere
                # so we contact replicas first but include all hosts (just in case)
                missing = tuple(x for x in self.hosts if x not in replicas)
                hosts = replicas + missing
                return hosts
        # if no _route_, return hosts
        return self.hosts

    def refresh_shard_map(self):
        self.logger.debug("commencing shard map refresh")
        cluster_data = self.solr.collections.cluster_status_raw()
        shard_map = {}
        for coll_name, coll_config in cluster_data['cluster']['collections'].items():
            coll = {}
            shards = {}
            slots = []
            hash_shard = {}
            for shard_name, shard_config in coll_config['shards'].items():
                replicas = []
                leader_host = None
                # keep replicas index, sorted by leader!
                for replica_name, replica_config in shard_config['replicas'].items():
                    if replica_config['state'].lower() in ('active', 'recovering'):
                        # since each host ~has multiple shards/replicas, we cache the base-url
                        host = replica_config['base_url']
                        if not host.endswith('/'):
                            host += '/'
                        host = sys.intern(host)
                        # track which host has the leader
                        if replica_config.get('leader') == 'true':
                            leader_host = host
                        # don't add a host multiple times
                        if host not in replicas:
                            replicas.append(host)
                # move leader_host to the front of the list
                if leader_host:
                    leader_index = replicas.index(leader_host)
                    if leader_index != 0:  # move to beginning of list
                        replicas.insert(0, replicas.pop(leader_index))
                shards[shard_name] = tuple(replicas)
                start_key, end_key = shard_config['range'].split('-')
                slots.extend((start_key, end_key))
                hash_shard[start_key] = shard_name
                hash_shard[end_key] = shard_name
            coll['hash_to_shard'] = hash_shard
            coll['slots'] = sorted(slots)
            coll['shards'] = shards
            shard_map[coll_name] = coll
        self.save_shard_map(shard_map)
        return shard_map

    def save_shard_map(self, shard_map):
        """
        Saves the shard map. Users can change this function to change the storage of the shard-map.
        Example: storing it in shared memory on the server(to be used by multiple processes)
        or on a in-memory cache like redis.

        """
        self.shard_map = shard_map

    def get_shard_map(self, force_refresh=False):
        """
        You can change this function to get the shard-map from somewhere/somehow place else in conjuction with
        save_shard_map().

        """
        now = datetime.utcnow()
        if force_refresh is True or \
                        self.shard_map is None or \
                        (now - self.last_refresh).total_seconds() > self.refresh_ttl:
            self.last_refresh = now
            self.refresh_shard_map()
        return self.shard_map
