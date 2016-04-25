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

#logging.basicConfig(level=logging.DEBUG,format='%(asctime)s [%(levelname)s] (%(process)d) (%(threadName)-10s) [%(name)s] %(message)s')

class ClientTestIndexing(unittest.TestCase):
    #High Level Client Tests
    
    @classmethod
    def setUpClass(self):
        self.solr = SolrClient(test_config['SOLR_SERVER'][0], devel=True, auth=test_config['SOLR_CREDENTIALS'])
        self.rand_docs = RandomTestData()
        self.docs = self.rand_docs.get_docs(50)
        
        for field in test_config['collections']['copy_fields']:
            try:
                self.solr.schema.delete_copy_field(test_config['SOLR_COLLECTION'],field)
            except:
                pass
        for field in test_config['collections']['fields']:
            try:
                self.solr.schema.create_field(test_config['SOLR_COLLECTION'],field)
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
    
    @unittest.skip("Skipping for now")
    def test_access_without_auth(self):
        if not test_config['SOLR_CREDENTIALS'][0]:
            return
        solr = SolrClient(test_config['SOLR_SERVER'],devel=True)
        with self.assertRaises(ConnectionError) as cm:
            solr.query('SolrClient_unittest',{'q':'not_gonna_happen'})
            
    
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
    
    def test_indexing_conn_log(self):
        self.docs = self.rand_docs.get_docs(53)
        self.solr.index_json(test_config['SOLR_COLLECTION'],json.dumps(self.docs))
        self.commit()
        sleep(5)
        for doc in self.docs:
            logging.debug("Checking {}".format(doc['id']))
            self.assertEqual(self.solr.query(test_config['SOLR_COLLECTION'],{'q':'id:{}'.format(doc['id'])}).get_num_found(),1)
        logging.info(self.solr.transport._action_log)
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
        try:
            os.remove('temp_file.json.gz')
            os.remove('temp_file.json')
        except:
            pass
            
    
    def test_stream_file_gzip_file(self):
        self.docs = self.rand_docs.get_docs(60)
        with gzip.open('temp_file.json.gz','wb') as f:
            f.write(json.dumps(self.docs).encode('utf-8'))
        r = self.solr.stream_file(test_config['SOLR_COLLECTION'],'temp_file.json.gz')
        self.commit()
        r = self.solr.query(test_config['SOLR_COLLECTION'],{'q':'*:*'})
        self.assertEqual(r.get_num_found(),len(self.docs))
        self.delete_docs()
        self.commit()
        try:
            os.remove('temp_file.json.gz')
            os.remove('temp_file.json')
        except:
            pass
            
    @unittest.skip("Don't test remote indexing in travis")
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
        try:
            os.remove('temp_file.json.gz')
            os.remove('temp_file.json')
        except:
            pass

    def test_paging_query_with_rows(self):
        self.docs = self.rand_docs.get_docs(1000)
        with gzip.open('temp_file.json.gz','wb') as f:
            f.write(json.dumps(self.docs).encode('utf-8'))
        r = self.solr.stream_file(test_config['SOLR_COLLECTION'],'temp_file.json.gz')
        self.commit()
        queries = 0
        docs = []
        for res in self.solr.paging_query(test_config['SOLR_COLLECTION'],{'q':'*:*'}, rows=50):
            self.assertTrue(len(res.docs) == 50)
            docs.extend(res.docs)
            queries +=1
        self.assertEqual(
            [x['id'] for x in sorted(docs, key= lambda x: x['id'])],
            [x['id'] for x in sorted(self.docs, key= lambda x: x['id'])]
            )
        self.assertTrue(1000/50 == queries)
        self.delete_docs()
        self.commit()
        try:
            os.remove('temp_file.json.gz')
            os.remove('temp_file.json')
        except:
            pass   

    def test_paging_query(self):
        self.docs = self.rand_docs.get_docs(1000)
        with gzip.open('temp_file.json.gz','wb') as f:
            f.write(json.dumps(self.docs).encode('utf-8'))
        r = self.solr.stream_file(test_config['SOLR_COLLECTION'],'temp_file.json.gz')
        self.commit()
        queries = 0
        docs = []
        for res in self.solr.paging_query(test_config['SOLR_COLLECTION'],{'q':'*:*'}):
            self.assertTrue(len(res.docs) == 1000)
            docs.extend(res.docs)
            queries +=1
        self.assertTrue(queries == 1)
        self.assertEqual(
            [x['id'] for x in sorted(docs, key= lambda x: x['id'])],
            [x['id'] for x in sorted(self.docs, key= lambda x: x['id'])]
            )
        self.delete_docs()
        self.commit()
        try:
            os.remove('temp_file.json.gz')
            os.remove('temp_file.json')
        except:
            pass              
            
    def test_paging_query_with_max(self):
        self.docs = self.rand_docs.get_docs(1000)
        with gzip.open('temp_file.json.gz','wb') as f:
            f.write(json.dumps(self.docs).encode('utf-8'))
        r = self.solr.stream_file(test_config['SOLR_COLLECTION'],'temp_file.json.gz')
        self.commit()
        queries = 0
        docs = []
        for res in self.solr.paging_query(test_config['SOLR_COLLECTION'], {'q':'*:*'}, rows = 50, max_start = 502):
            self.assertTrue(len(res.docs) == 50)
            queries +=1
            docs.extend(res.docs)
        ids = [x['id'] for x in docs]

        for item in docs:
            self.assertTrue(item['id'] in ids)

        self.assertEqual(11, queries)
        self.delete_docs()
        self.commit()
        try:
            os.remove('temp_file.json.gz')
            os.remove('temp_file.json')
        except:
            pass    
if __name__=='__main__':
    pass
  
