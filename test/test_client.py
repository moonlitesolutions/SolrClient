import unittest
import gzip
import logging
import json
import os
from time import sleep
from SolrClient import SolrClient
from .test_config import test_config
from .RandomTestData import RandomTestData
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s [%(levelname)s] (%(process)d) (%(threadName)-10s) [%(name)s] %(message)s')

class ClientTestIndexing(unittest.TestCase):
    #High Level Client Tests
    
    @classmethod
    def setUpClass(self):
        self.solr = SolrClient(test_config['SOLR_SERVER'][0],devel=True)
        self.rand_docs = RandomTestData()
        self.docs = self.rand_docs.get_docs(50)
        
        for field in test_config['collections']['copy_fields']:
            try:
                self.solr.collections.delete_copy_field(test_config['SOLR_COLLECTION'],field)
            except:
                pass
        for field in test_config['collections']['fields']:
            try:
                self.solr.collections.create_field(test_config['SOLR_COLLECTION'],field)
            except:
                pass
                
    def setUp(self):
        self.delete_docs()
        self.commit()
    
    def delete_docs(self):
        self.solr.delete_doc_by_id(test_config['SOLR_COLLECTION'],'*')
        self.commit()
        
    def commit(self):
        self.solr.commit(test_config['SOLR_COLLECTION'],openSearcher=True)
        sleep(5)

    def test_indexing_json(self):
        self.docs = self.rand_docs.get_docs(53)
        self.solr.index_json(test_config['SOLR_COLLECTION'],json.dumps(self.docs))
        self.commit()
        sleep(5)
        for doc in self.docs:
            logging.debug("Checking {}".format(doc['id']))
            self.assertEqual(self.solr.query(test_config['SOLR_COLLECTION'],{'q':'id:{}'.format(doc['id'])}).get_num_found(),1)
        self.delete_docs()
        self.commit()
        

    def test_index_json_file(self):
        self.docs = self.rand_docs.get_docs(55)
        with open('temp_file.json','w') as f:
            json.dump(self.docs,f)
        r = self.solr.stream_file(test_config['SOLR_COLLECTION'],'temp_file.json')
        self.commit()
        r = self.solr.query(test_config['SOLR_COLLECTION'],{'q':'*:*'})
        self.assertEqual(r.get_num_found(),len(self.docs))
        self.delete_docs()
        self.commit()
        os.remove('temp_file.json')
    
    def test_stream_file_gzip_file(self):
        self.docs = self.rand_docs.get_docs(60)
        with gzip.open('temp_file.json.gz','wt') as f:
            json.dump(self.docs,f)
        r = self.solr.stream_file(test_config['SOLR_COLLECTION'],'temp_file.json.gz')
        self.commit()
        r = self.solr.query(test_config['SOLR_COLLECTION'],{'q':'*:*'})
        self.assertEqual(r.get_num_found(),len(self.docs))
        self.delete_docs()
        self.commit()
        os.remove('temp_file.json.gz')
        
    def test_index_json_file(self):
        self.docs = self.rand_docs.get_docs(61)
        with open('temp_file.json','w') as f:
            json.dump(self.docs,f)
        r = self.solr.local_index(test_config['SOLR_COLLECTION'],'temp_file.json')
        self.commit()
        r = self.solr.query(test_config['SOLR_COLLECTION'],{'q':'*:*'})
        self.assertEqual(r.get_num_found(),len(self.docs))
        self.delete_docs()
        self.commit()
        os.remove('temp_file.json')
    
    def test_stream_file_gzip_file(self):
        self.docs = self.rand_docs.get_docs(62)
        with gzip.open('temp_file.json.gz','wt') as f:
            json.dump(self.docs,f)
        r = self.solr.local_index(test_config['SOLR_COLLECTION'],'temp_file.json.gz')
        self.commit()
        r = self.solr.query(test_config['SOLR_COLLECTION'],{'q':'*:*'})
        self.assertEqual(r.get_num_found(),len(self.docs))
        self.delete_docs()
        self.commit()
        os.remove('temp_file.json.gz')
if __name__=='__main__':
    pass
  
