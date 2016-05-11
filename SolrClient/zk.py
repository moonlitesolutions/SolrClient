import json
import os
import logging
from .transport import TransportRequests
from .schema import Schema
from .exceptions import *
from .solrresp import SolrResponse
try:
    from kazoo.client import KazooClient
    import kazoo
    kz_imported = True
except ImportError:
    kz_imported = False


class ZK():
    def __init__(self, solr, log):
        '''
        Helper class for working with Solr Zookeeper config and collections. 

        Still very experimental, I wouldn't even use it myself if I didn't like to live on the edge. 
        '''
        if not kz_imported:
            raise ImportError("To use the ZK Class you need to have Kazoo Client installed")
        self.solr = solr
        self.logger = log
        try:
            self.system_data = self.solr.transport.send_request(endpoint='admin/info/system', params={'wt':'json'})
            self.zk_hosts = self.system_data[0]['zkHost']
        except Exception as e:
            self.logger.error("Couldn't get System info From Solr or bad format.")
            self.logger.exception(e)
            raise
        try: 
            self.kz = KazooClient(hosts=self.zk_hosts)
            self.kz.start()
            if self.kz.state != 'CONNECTED':
                self.logger.error("Couldn't establish connection to Zookeeper")
        except Exception as e:
            self.logger.error("Couldn't Establish Connection To Zookeeper")
            raise(e)

    def check_zk(self):
        '''
        Will attempt to telnet to each zookeeper that is used by SolrClient and issue 'mntr' command. Response is parsed to check to see if the 
        zookeeper node is a leader or a follower and returned as a dict. 

        If the telnet collection fails or the proper response is not parsed, the zk node will be listed as 'down' in the dict. Desired values are
        either follower or leader. 
        '''
        import telnetlib
        temp = self.zk_hosts.split('/')
        zks = temp[0].split(',')
        status = {}
        for zk in zks:
            self.logger.debug("Checking {}".format(zk))
            host, port = zk.split(':')
            try:
                t = telnetlib.Telnet(host, port=int(port))
                t.write('mntr'.encode('ascii'))
                r = t.read_all()
                for out in r.decode('utf-8').split('\n'):
                    if out:
                        param, val = out.split('\t')
                        if param == 'zk_server_state':
                            status[zk] = val
            except Exception as e:
                self.logger.error("Unable to reach ZK: {}".format(zk))
                self.logger.exception(e)
                status[zk] = 'down'
        #assert len(zks) == len(status)
        return status
        
    def _get_path(self, path):
        if self.kz.exists(path):
            return self.kz.get(path)

            
    def _copy_dir(self, src, dst):
        self.logger.debug("Copying ZK Nodes {} -> {}".format(src, dst))
        if not self.kz.exists(dst):
            self.kz.create(dst)
        children = self.kz.get_children(src)
        for child in children:
            self.logger.debug("Copying {} to {}".format(src+'/'+child, dst+'/'+child))
            node = self.kz.get(src+"/{}".format(child))
            if node[1].numChildren > 0 and node[0] is None:
                self._copy_dir(src+"/{}".format(child), dst+"/{}".format(child))
            else:
                try:
                    self.kz.delete(dst+"/{}".format(child))
                except kazoo.exceptions.NoNodeError:
                    #Doesn't exist
                    pass
                self.kz.create(dst+"/{}".format(child),node[0])


    def copy_config(self, original, new):
        '''
        Copies collection configs into a new folder. Can be used to create new collections based on existing configs. 

        Basically, copies all nodes under /configs/original to /configs/new.

        :param original str: ZK name of original config
        :param new str: New name of the ZK config. 
        '''
        if not self.kz.exists('/configs/{}'.format(original)):
            raise ZookeeperError("Collection doesn't exist in Zookeeper. Current Collections are: {}".format(self.kz.get_children('/configs')))
        base = '/configs/{}'.format(original)
        nbase = '/configs/{}'.format(new)
        self._copy_dir(base, nbase)

    
    def download_collection_configs(self, collection, fs_path):
        '''
        Downloads ZK Directory to the FileSystem.

        :param collection str: Name of the collection (zk config name)
        :param fs_path str: Destination filesystem path. 
        '''
        
        if not self.kz.exists('/configs/{}'.format(collection)):
            raise ZookeeperError("Collection doesn't exist in Zookeeper. Current Collections are: {} ".format(self.kz.get_children('/configs')))
        self._download_dir('/configs/{}'.format(collection), fs_path + os.sep + collection)

        
    def _download_dir(self, src, dst):
        if not self.kz.exists(src):
            raise ZookeeperError("Source Directory {} Doesn't exist".format(src))
        if not os.path.isdir(dst):
            os.makedirs(dst)
        children = self.kz.get_children(src)
        for child in children:
            node = self.kz.get(src+"/{}".format(child))
            if node[1].numChildren > 0 and node[0] is None:
                self._download_dir(src+"/{}".format(child), dst+"{}{}".format(os.sep, child))
            else:
                self.logger.debug("Copying {} to {}".format(src+'/'+child, dst + os.sep + child))     
                f = open(dst + os.sep + child, 'w')
                f.write(node[0].decode('utf-8'))
                f.close()

                
    def upload_collection_configs(self, collection, fs_path):
        '''
        Uploads collection configurations from a specified directory to zookeeper. 
        
        '''
        coll_path = fs_path
        if not os.path.isdir(coll_path):
            raise ValueError("{} Doesn't Exist".format(coll_path))
        self._upload_dir(coll_path, '/configs/{}'.format(collection))

        
    def _upload_dir(self, src, dst):
        if not self.kz.exists(dst):
            self.logger.info("Creating ZK Path {}".format(dst))
            self.kz.create(dst)
        children = os.listdir(src)
        for child in children:
            if os.path.isfile(src + os.sep + child):
                with open(src + os.sep + child) as f:
                    data = f.read()
                if self.kz.exists(dst+'/{}'.format(child)):
                    self.kz.delete(dst+'/{}'.format(child))
                self.kz.create(dst+'/{}'.format(child), bytes(data, encoding='utf-8'))
                self.logger.debug("Created {}".format(child))
            else:
                self._upload_dir(src+"/{}".format(child), dst+"/{}".format(child))
                
    def get_item(self, path):
        '''
        Returns a specified zookeeper node. 
        '''
        return self.kz.get(path)
    