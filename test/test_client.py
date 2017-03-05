import unittest
import gzip
import logging
import json
import os
from SolrClient import SolrClient
from SolrClient.exceptions import *
from SolrClient.routers.aware import AwareRouter
from .test_config import test_config
from .RandomTestData import RandomTestData

#logging.basicConfig(level=logging.DEBUG,format='%(asctime)s [%(levelname)s] (%(process)d) (%(threadName)-10s) [%(name)s] %(message)s')
logging.disable(logging.CRITICAL)


class ClientTestIndexing(unittest.TestCase):
    @classmethod
    def get_solr(cls):
        return SolrClient(test_config['SOLR_SERVER'][0], devel=True, auth=test_config['SOLR_CREDENTIALS'])

    @classmethod
    def setUpClass(self):
        self.solr = self.get_solr()
        self.rand_docs = RandomTestData()
        self.docs = self.rand_docs.get_docs(50)

        for field in test_config['collections']['copy_fields']:
            try:
                self.solr.schema.delete_copy_field(test_config['SOLR_COLLECTION'], field)
            except Exception as e:
                pass

        for field in test_config['collections']['fields']:
            try:
                self.solr.schema.create_field(test_config['SOLR_COLLECTION'], field)
            except Exception as e:
                pass

    def setUp(self):
        self.delete_docs()
        self.commit()

    def delete_docs(self):
        self.solr.delete_doc_by_id(test_config['SOLR_COLLECTION'], '*')
        self.commit()

    def commit(self):
        # softCommit because we don't care about data on disk
        self.solr.commit(test_config['SOLR_COLLECTION'], openSearcher=True, softCommit=True)

    def test_down_solr_exception(self):
        # connect to "down" sorl host
        s = SolrClient('http://localhost:8999/solr', devel=True)
        with self.assertRaises(ConnectionError):
            s.query('test', {})

    def test_delete_doc_by_id_with_space(self):
        self.delete_docs()
        self.solr.index_json(test_config['SOLR_COLLECTION'], json.dumps(
            [{'id': 'potato potato', 'product_name': 'potato'}]))
        self.commit()
        self.assertTrue(
            len(self.solr.query(test_config['SOLR_COLLECTION'],
                                {'q': 'id:"potato potato"'}).docs) == 1)
        self.solr.delete_doc_by_id(test_config['SOLR_COLLECTION'],
                                   "potato potato")
        self.commit()
        self.assertTrue(
            len(self.solr.query(test_config['SOLR_COLLECTION'],
                                {'q': 'id:"potato potato"'}).docs) == 0)
        self.delete_docs()

    def test_delete_doc_by_query(self):
        self.delete_docs()
        self.solr.index_json(test_config['SOLR_COLLECTION'], json.dumps(
            [{'id': 'potato potato', 'product_name': 'potato'}]))
        self.commit()
        self.assertTrue(
            len(self.solr.query(test_config['SOLR_COLLECTION'],
                                {'q': 'id:"potato potato"'}).docs) == 1)
        self.solr.delete_doc_by_query(test_config['SOLR_COLLECTION'],
                                      "product_name:potato")
        self.commit()
        self.assertTrue(
            len(self.solr.query(test_config['SOLR_COLLECTION'],
                                {'q': 'id:"potato potato"'}).docs) == 0)
        self.delete_docs()

    @unittest.skip("Skipping for now")
    def test_access_without_auth(self):
        if not test_config['SOLR_CREDENTIALS'][0]:
            return
        solr = SolrClient(test_config['SOLR_SERVER'], devel=True)
        with self.assertRaises(ConnectionError) as cm:
            solr.query('SolrClient_unittest', {'q': 'not_gonna_happen'})

    def test_indexing_json(self):
        self.docs = self.rand_docs.get_docs(53)
        self.solr.index_json(test_config['SOLR_COLLECTION'],
                             json.dumps(self.docs))
        self.commit()
        for doc in self.docs:
            logging.debug("Checking {}".format(doc['id']))
            self.assertEqual(
                self.solr.query(test_config['SOLR_COLLECTION'], {'q': 'id:{}'.format(doc['id'])}).get_num_found(), 1)
        self.delete_docs()
        self.commit()

    def test_indexing(self):
        self.docs = self.rand_docs.get_docs(53)
        self.solr.index(test_config['SOLR_COLLECTION'], self.docs)
        self.commit()
        for doc in self.docs:
            logging.debug("Checking {}".format(doc['id']))
            self.assertEqual(
                self.solr.query(test_config['SOLR_COLLECTION'], {'q': 'id:{}'.format(doc['id'])}).get_num_found(), 1)

    def test_index_min_rf(self):
        # we don't have 200 replicas, so it will always fail to fulfill min_rf
        with self.assertRaises(MinRfError):
            self.solr.index(test_config['SOLR_COLLECTION'], [self.docs[0]], min_rf=200)
        self.commit()  # commit because it wrote it on active shards and we need to delete for the next test!
        self.delete_docs()
        self.commit()

    def test_router_aware(self):
        s = self.get_solr()
        s.router = AwareRouter(s, s.host)
        # check shard map get's built without error
        s.router.refresh_shard_map()
        # check dumb query to see stuff isn't broken
        s.query(test_config['SOLR_COLLECTION'], {}, _route_='1', prefer_leader=True)
        # todo needs something better, to check that the shard selected is the right one based on _route_ key

    def test_get(self):
        doc_id = '1'
        self.solr.index_json(test_config['SOLR_COLLECTION'], json.dumps([{'id': doc_id}]))
        # this returns the doc!
        self.solr.get(test_config['SOLR_COLLECTION'], doc_id)
        with self.assertRaises(NotFoundError):
            self.solr.get(test_config['SOLR_COLLECTION'], '5')

    def test_mget(self):
        self.solr.index_json(test_config['SOLR_COLLECTION'], json.dumps([{'id': '1'}]))
        self.solr.index_json(test_config['SOLR_COLLECTION'], json.dumps([{'id': '5'}]))
        docs = self.solr.mget(test_config['SOLR_COLLECTION'], ('5', '1'))
        self.assertEqual(len(docs), 2)

    def test_indexing_conn_log(self):
        self.docs = self.rand_docs.get_docs(53)
        self.solr.index_json(test_config['SOLR_COLLECTION'], json.dumps(self.docs))
        self.commit()
        for doc in self.docs:
            logging.debug("Checking {}".format(doc['id']))
            self.assertEqual(
                self.solr.query(test_config['SOLR_COLLECTION'], {'q': 'id:{}'.format(doc['id'])}).get_num_found(), 1)
        logging.info(self.solr.transport._action_log)
        self.delete_docs()
        self.commit()


    def test_indexing(self):
        self.docs = self.rand_docs.get_docs(53)
        self.solr.index(test_config['SOLR_COLLECTION'], self.docs)
        self.commit()
        for doc in self.docs:
            logging.debug("Checking {}".format(doc['id']))
            self.assertEqual(
                self.solr.query(test_config['SOLR_COLLECTION'], {'q': 'id:{}'.format(doc['id'])}).get_num_found(), 1)
        logging.info(self.solr.transport._action_log)
        self.delete_docs()
        self.commit()


    def test_index_json_file(self):
        self.docs = self.rand_docs.get_docs(55)
        with open('temp_file.json', 'w') as f:
            json.dump(self.docs, f)
        r = self.solr.stream_file(test_config['SOLR_COLLECTION'], 'temp_file.json')
        self.commit()
        r = self.solr.query(test_config['SOLR_COLLECTION'], {'q': '*:*'})
        self.assertEqual(r.get_num_found(), len(self.docs))
        self.delete_docs()
        self.commit()
        try:
            os.remove('temp_file.json.gz')
            os.remove('temp_file.json')
        except:
            pass

    def test_stream_file_gzip_file(self):
        self.docs = self.rand_docs.get_docs(60)
        with gzip.open('temp_file.json.gz', 'wb') as f:
            f.write(json.dumps(self.docs).encode('utf-8'))
        r = self.solr.stream_file(test_config['SOLR_COLLECTION'], 'temp_file.json.gz')
        self.commit()
        r = self.solr.query(test_config['SOLR_COLLECTION'], {'q': '*:*'})
        self.assertEqual(r.get_num_found(), len(self.docs))
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
        with open('temp_file.json', 'w') as f:
            json.dump(self.docs, f)
        r = self.solr.local_index(test_config['SOLR_COLLECTION'], 'temp_file.json')
        self.commit()
        r = self.solr.query(test_config['SOLR_COLLECTION'], {'q': '*:*'})
        self.assertEqual(r.get_num_found(), len(self.docs))
        self.delete_docs()
        self.commit()
        try:
            os.remove('temp_file.json.gz')
            os.remove('temp_file.json')
        except:
            pass

    def test_paging_query_with_rows(self):
        self.docs = self.rand_docs.get_docs(1000)
        with gzip.open('temp_file.json.gz', 'wb') as f:
            f.write(json.dumps(self.docs).encode('utf-8'))
        r = self.solr.stream_file(test_config['SOLR_COLLECTION'],
                                  'temp_file.json.gz')
        self.commit()
        queries = 0
        docs = []
        for res in self.solr.paging_query(test_config['SOLR_COLLECTION'],
                                          {'q': '*:*'},
                                          rows=50):
            self.assertTrue(len(res.docs) == 50)
            docs.extend(res.docs)
            queries += 1
        self.assertEqual(
            [x['id'] for x in sorted(docs, key=lambda x: x['id'])],
            [x['id'] for x in sorted(self.docs, key=lambda x: x['id'])]
        )
        self.assertTrue(1000 / 50 == queries)
        self.delete_docs()
        self.commit()
        try:
            os.remove('temp_file.json.gz')
            os.remove('temp_file.json')
        except:
            pass

    def test_paging_query(self):
        self.docs = self.rand_docs.get_docs(1000)
        with gzip.open('temp_file.json.gz', 'wb') as f:
            f.write(json.dumps(self.docs).encode('utf-8'))
        r = self.solr.stream_file(test_config['SOLR_COLLECTION'],
                                  'temp_file.json.gz')
        self.commit()
        queries = 0
        docs = []
        for res in self.solr.paging_query(test_config['SOLR_COLLECTION'],
                                          {'q': '*:*'}):
            self.assertTrue(len(res.docs) == 1000)
            docs.extend(res.docs)
            queries += 1
        self.assertTrue(queries == 1)
        self.assertEqual(
            [x['id'] for x in sorted(docs, key=lambda x: x['id'])],
            [x['id'] for x in sorted(self.docs, key=lambda x: x['id'])]
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
        with gzip.open('temp_file.json.gz', 'wb') as f:
            f.write(json.dumps(self.docs).encode('utf-8'))
        r = self.solr.stream_file(test_config['SOLR_COLLECTION'], 'temp_file.json.gz')
        self.commit()
        queries = 0
        docs = []
        for res in self.solr.paging_query(test_config['SOLR_COLLECTION'], {'q': '*:*'}, rows=50, max_start=502):
            self.assertTrue(len(res.docs) == 50)
            queries += 1
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

    def test_cursor_query(self):
        self.docs = self.rand_docs.get_docs(2000)
        with gzip.open('temp_file.json.gz', 'wb') as f:
            f.write(json.dumps(self.docs).encode('utf-8'))
        r = self.solr.stream_file(test_config['SOLR_COLLECTION'], 'temp_file.json.gz')
        self.commit()
        queries = 0
        docs = []

        for res in self.solr.cursor_query(test_config['SOLR_COLLECTION'], {'q': '*:*', 'rows': 100}):
            self.assertTrue(len(res.docs) == 100)
            queries += 1
            docs.extend(res.docs)

        ids = [x['id'] for x in docs]

        for item in docs:
            self.assertTrue(item['id'] in ids)

        self.delete_docs()
        self.commit()
        try:
            os.remove('temp_file.json.gz')
            os.remove('temp_file.json')
        except:
            pass


if __name__ == '__main__':
    pass
