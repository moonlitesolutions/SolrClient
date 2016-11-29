import unittest
import logging
import os
import random
from SolrClient import SolrClient
from SolrClient.exceptions import *
from .test_config import test_config
from .RandomTestData import RandomTestData

# logging.basicConfig(level=logging.DEBUG,format='%(asctime)s [%(levelname)s] (%(process)d) (%(threadName)-10s) [%(name)s] %(message)s')
logging.disable(logging.CRITICAL)


class ZKTest(unittest.TestCase):
    # High level zk tests

    @classmethod
    def setUpClass(self):
        self.solr = SolrClient(test_config['SOLR_SERVER'][0],
                               devel=True,
                               auth=test_config['SOLR_CREDENTIALS'])
        self.rand_docs = RandomTestData()
        self.docs = self.rand_docs.get_docs(50)
        self.coll = test_config['SOLR_COLLECTION'] + str(random.random() * 100)
        self.temp_dir = test_config['temp_data']
        res, con_info = self.solr.collections.api('create', {
            'name': self.coll,
            'numShards': 1,
            'replicationFactor': 1,
            'collection.configName': 'basic_configs'
        })
        self.zk = self.solr.get_zk()

    @classmethod
    def tearDownClass(self):
        res, con_info = self.solr.collections.api('delete', {'name': self.coll})

    def test_zk_get_collection_config_bad_collection(self):
        with self.assertRaises(ZookeeperError):
            self.zk.download_collection_configs('asdasdasd', self.temp_dir + os.sep + self.coll)

    def test_zk_copy_config(self):
        a = self.zk.copy_config('basic_configs', 'new_config')
        self.assertTrue(self.zk.kz.get('/configs/new_config'))
        self.zk.kz.delete('/configs/new_config', recursive=True)

    def test_download_collection_configs(self):
        # really bad test, need to rework later
        a = self.zk.download_collection_configs('basic_configs',
                                                self.temp_dir + '/configs')
        self.assertTrue(os.path.isdir(self.temp_dir + '/configs'))

    def test_upload_collection_configs(self):
        a = self.zk.upload_collection_configs('test1', self.temp_dir + '/configs/basic_configs')
        self.zk.kz.delete('/configs/test1', recursive=True)


if __name__ == '__main__':
    pass
