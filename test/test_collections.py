import unittest
import gzip
import logging
import json
import os
from time import sleep
from SolrClient import SolrClient
from SolrClient.exceptions import *
from .test_config import test_config
from .RandomTestData import RandomTestData
from unittest.mock import Mock
import random

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s [%(levelname)s] (%(process)d) (%(threadName)-10s) [%(name)s] %(message)s')

class test_collections(unittest.TestCase):
    #High Level Client Tests
    def setUp(self):
        self.solr = SolrClient(test_config['SOLR_SERVER'][0], devel=True)
  
    def test_create_collection_no_args(self):
        with self.assertRaises(SolrError):
            self.solr.collections.api('action', {})
            
    def test_create_collection(self):
        temp = test_config['SOLR_COLLECTION']+str(random.random()*100)
        
        res, con_info = self.solr.collections.api('create', {
                                'name': temp,
                                'numShards': 1,
                                'replicationFactor': 1,
                                'collection.configName':'basic_configs'
                                })
        self.assertTrue('success' in res)
        with self.assertRaises(SolrError):
            #Make sure error is returned if I try to create collection with the same name
            res, con_info = self.solr.collections.api('create', {
                                'name': temp,
                                'numShards': 1,
                                'replicationFactor': 1,
                                'collection.configName':'basic_configs'
                                })
        
        #Clean up and delete the collection
        res, con_info = self.solr.collections.api('delete', {'name':temp})
        self.assertTrue('success' in res)
    
    def test_get_clusterstatus(self):
        c = self.solr.collections.clusterstatus()
        self.assertTrue(type(c) is dict)
        self.assertTrue(len(c.keys()) > 1)
        
    def test_get_collection_counts(self):
        c = self.solr.collections.get_collection_counts()
        self.assertTrue(type(c) is dict)
        self.assertTrue(len(c.keys()) > 1)

                
    def test__check_shard_count1(self):
        self.assertFalse(
            self.solr.collections._check_shard_status( {'core_node2': {'state': 'active', 'doc_count': 6453698}, 
                                                        'core_node3': {'state': 'down', 'doc_count': False}}))
        self.assertTrue(
            self.solr.collections._check_shard_status( {'core_node2': {'state': 'active', 'doc_count': 6453698}, 
                                                        'core_node3': {'state': 'down', 'doc_count': 6453698}}))
        
    def test__check_shard_status1(self):
        self.assertFalse(
            self.solr.collections._check_shard_status( {'core_node2': {'state': 'active', 'doc_count': 6453698}, 
                                                        'core_node3': {'state': 'down', 'doc_count': False}}))
        
        self.assertTrue(
            self.solr.collections._check_shard_status( {'core_node2': {'state': 'active', 'doc_count': 6453698}, 
                                                        'core_node3': {'state': 'active', 'doc_count': False}}))
if __name__=='__main__':
    pass