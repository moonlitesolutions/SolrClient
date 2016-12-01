import unittest
import gzip
import logging
import json
import os
import random
from multiprocessing.pool import ThreadPool
from SolrClient import SolrClient, IndexQ
from SolrClient.exceptions import *
from .test_config import test_config
from .RandomTestData import RandomTestData
import shutil
from functools import partial
from datetime import datetime as dt
from time import sleep
test_config['indexqbase'] = os.getcwd()

logging.disable(logging.CRITICAL)


class TestIndexQ(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.rand_docs = RandomTestData()
        self.docs = self.rand_docs.get_docs(50)
        self.devel = False
        if self.devel:
            logging.basicConfig(level=logging.DEBUG,
                                format='%(asctime)s [%(levelname)s] (%(process)d) (%(threadName)-10s) [%(name)s] %(message)s')
        try:
            shutil.rmtree(test_config['indexqbase'] + os.sep + 'testq')
        except:
            pass

    @classmethod
    def tearDownClass(self):
        try:
            shutil.rmtree(test_config['indexqbase'] + os.sep + 'testq')
        except:
            pass

    def setUp(self):
        index = IndexQ(test_config['indexqbase'], 'testq')
        for dir in ['_todo_dir', '_done_dir']:
            [os.remove(x) for x in index.get_all_as_list(dir=dir)]

    def check_file_contents(self, file_path, real_data):
        if os.path.isfile(file_path):
            if file_path.endswith('.gz'):
                f = gzip.open(file_path, 'rt', encoding='utf-8')
                f_data = json.load(f)
            else:
                f = open(file_path)
                f_data = json.load(f)
            f.close()
        [self.assertEqual(f_data[x], real_data[x]) for x in range(len(real_data))]

    def test_add_bad_list(self):
        index = IndexQ(test_config['indexqbase'], 'testq')
        with self.assertRaises(ValueError):
            index.add([{}, {}, [], {}])

    def test_add_string(self):
        index = IndexQ(test_config['indexqbase'], 'testq')
        string_test = 'asd'
        doc = index.add(string_test)
        with open(doc) as f:
            doc_data = f.read()
        self.assertEqual(string_test, doc_data)

    def test_add_int(self):
        index = IndexQ(test_config['indexqbase'], 'testq')
        with self.assertRaises(ValueError):
            index.add(1)

    def test_add_good_dict_zero_size(self):
        index = IndexQ(test_config['indexqbase'], 'testq')
        doc = index.add(self.docs[0])
        # Sending docs as list because that is how json is stored
        self.check_file_contents(doc, [self.docs[0]])

    def test_add_good_list_zero_size(self):
        index = IndexQ(test_config['indexqbase'], 'testq')
        doc = index.add(self.docs[0:20])
        self.check_file_contents(doc, self.docs[0:20])

    def test_add_good_list_zero_size_compressed(self):
        index = IndexQ(test_config['indexqbase'], 'testq', compress=True)
        doc = index.add(self.docs[0:20])
        self.check_file_contents(doc, self.docs[0:20])

    def test_add_good_dict_zero_size(self):
        index = IndexQ(test_config['indexqbase'], 'testq', compress=True)
        doc = index.add(self.docs[0])
        # Sending docs as list because that is how json is stored
        self.check_file_contents(doc, [self.docs[0]])

    def test_buffer_list_1m(self):
        size = 1
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        while True:
            doc = index.add(self.docs)
            [buff.append(x) for x in self.docs]
            if type(doc) is str:
                break
        self.check_file_contents(doc, buff)
        self.assertLessEqual(os.path.getsize(doc), size * 1000000)
        self.assertGreaterEqual(os.path.getsize(doc), size * 1000000 * .90)
        os.remove(doc)

    def test_buffer_dict_1m(self):
        size = 1
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        while True:
            item = random.choice(self.docs)
            doc = index.add(item)
            buff.append(item)
            if type(doc) is str:
                break
        self.check_file_contents(doc, buff)
        self.assertLessEqual(os.path.getsize(doc), size * 1000000)
        self.assertGreaterEqual(os.path.getsize(doc), size * 1000000 * .90)
        os.remove(doc)

    def test_buffer_dict_25m(self):
        size = 25
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        while True:
            item = random.choice(self.docs)
            doc = index.add(item)
            buff.append(item)
            if type(doc) is str:
                break
        self.check_file_contents(doc, buff)
        self.assertLessEqual(os.path.getsize(doc), size * 1000000)
        self.assertGreaterEqual(os.path.getsize(doc), size * 1000000 * .90)
        os.remove(doc)

    def test_buffer_list_25m(self):
        size = 25
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        while True:
            doc = index.add(self.docs)
            [buff.append(x) for x in self.docs]
            if type(doc) is str:
                break
        self.check_file_contents(doc, buff)
        self.assertLessEqual(os.path.getsize(doc), size * 1000000)
        self.assertGreaterEqual(os.path.getsize(doc), size * 1000000 * .90)
        os.remove(doc)

    def test_buffer_dict_75m(self):
        size = 75
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        while True:
            item = random.choice(self.docs)
            doc = index.add(item)
            buff.append(item)
            if type(doc) is str:
                break
        self.check_file_contents(doc, buff)
        self.assertLessEqual(os.path.getsize(doc), size * 1000000)
        self.assertGreaterEqual(os.path.getsize(doc), size * 1000000 * .90)
        os.remove(doc)

    def test_buffer_list_75m(self):
        size = 75
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        while True:
            doc = index.add(self.docs)
            [buff.append(x) for x in self.docs]
            if type(doc) is str:
                break
        self.check_file_contents(doc, buff)
        self.assertLessEqual(os.path.getsize(doc), size * 1000000)
        self.assertGreaterEqual(os.path.getsize(doc), size * 1000000 * .90)
        os.remove(doc)

    def test_buffer_list_75m_dump_early(self):
        size = 75
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        while True:
            doc = index.add(self.docs)
            [buff.append(x) for x in self.docs]
            if doc > 40000000:
                doc = index.add(finalize=True)
            if type(doc) is str:
                break
        self.check_file_contents(doc, buff)
        os.remove(doc)

    def test_by_get_all_compressed(self):
        size = 1
        files = 2
        index = IndexQ(test_config['indexqbase'], 'testq', size=size, compress=True)
        buff = []
        docs = []
        for _ in range(files):
            doc = index.add(self.docs, finalize=True)
            docs.append(doc)
            sleep(1)
        index = IndexQ(test_config['indexqbase'], 'testq')
        indexdocs = index.get_all_as_list()
        self.assertEqual(docs, indexdocs)
        [os.remove(doc) for doc in docs]

    def test_by_get_all_no_compressed(self):
        size = 1
        files = 2
        index = IndexQ(test_config['indexqbase'], 'testq', size=size, compress=False)
        buff = []
        docs = []
        for _ in range(files):
            doc = index.add(self.docs, finalize=True)
            docs.append(doc)
            sleep(1)
        index = IndexQ(test_config['indexqbase'], 'testq', mode='out')
        indexdocs = index.get_all_as_list()
        self.assertEqual(docs, indexdocs)
        [os.remove(doc) for doc in docs]

    def test_by_get_all_default_compression(self):
        size = 1
        files = 2
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        docs = []
        for _ in range(files):
            doc = index.add(self.docs, finalize=True)
            docs.append(doc)
            sleep(1)
        index = IndexQ(test_config['indexqbase'], 'testq', mode='out')
        indexdocs = index.get_all_as_list()
        self.assertEqual(docs, indexdocs)
        [os.remove(doc) for doc in docs]

    def test_dequeue(self):
        size = 1
        files = 2
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        docs = []
        for _ in range(files):
            doc = index.add(self.docs, finalize=True)
            docs.append(doc)
            sleep(1)
        index = IndexQ(test_config['indexqbase'], 'testq')
        indexdocs = []
        for x in index.get_todo_items():
            indexdocs.append(x)
        self.assertEqual(docs, indexdocs)
        [os.remove(doc) for doc in docs]

    def test_dequeue_100(self):
        size = 1
        files = 100
        rdocs = self.rand_docs.get_docs(500)
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        docs = []
        for dir in ['_todo_dir', '_done_dir']:
            [os.remove(x) for x in index.get_all_as_list(dir=dir)]
        for _ in range(files):
            doc = index.add(rdocs, finalize=True)
            docs.append(doc)
            sleep(1)
        index = IndexQ(test_config['indexqbase'], 'testq')
        indexdocs = []
        for x in index.get_todo_items():
            indexdocs.append(x)
        self.assertEqual(docs, indexdocs)
        [os.remove(doc) for doc in docs]

    def test_dequeue_and_complete_no_compression_5(self):
        size = 1
        files = 5
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        docs = []
        for _ in range(files):
            doc = index.add(self.docs, finalize=True)
            docs.append(doc)
            sleep(1)
        index = IndexQ(test_config['indexqbase'], 'testq', compress=False)
        indexdocs = []
        for x in index.get_todo_items():
            indexdocs.append(x)
            index.complete(x)
        self.assertEqual(docs, indexdocs)

        finaldocnames = [os.path.split(x)[-1] for x in indexdocs]
        donefilepaths = [os.path.join(index._done_dir, x) for x in finaldocnames]
        for x in donefilepaths:
            self.assertTrue(os.path.exists(x))
        [os.remove(doc) for doc in donefilepaths]

    def test_locking(self):
        '''
        Working on this one, it doesn't lock properly
        '''
        files = 5
        index = IndexQ(test_config['indexqbase'], 'testq')
        buff = []
        docs = []
        for _ in range(files):
            doc = index.add(self.docs, finalize=True)
            docs.append(doc)
            sleep(1)
        index = IndexQ(test_config['indexqbase'], 'testq', mode='out', devel=True)
        x = index.get_todo_items()
        self.assertTrue(os.path.exists(index._lck))
        with self.assertRaises(RuntimeError) as a:
            new_index = IndexQ(test_config['indexqbase'], 'testq', mode='out')
            y = new_index.get_todo_items()
        [index.complete(i) for i in x]
        self.assertFalse(os.path.exists(index._lck))

    def test_index(self):
        index = IndexQ(test_config['indexqbase'], 'testq')
        solr = SolrClient(test_config['SOLR_SERVER'],
                          devel=True,
                          auth=test_config['SOLR_CREDENTIALS'])
        solr.delete_doc_by_id(test_config['SOLR_COLLECTION'], '*')
        buff = []
        files = []
        for doc in self.docs:
            files.append(index.add(doc, finalize=True))
        index.index(solr, test_config['SOLR_COLLECTION'])
        solr.commit(test_config['SOLR_COLLECTION'], openSearcher=True)
        for doc in self.docs:
            res = solr.query(test_config['SOLR_COLLECTION'], {'q': 'id:{}'.format(doc['id'])})
            self.assertTrue(res.get_results_count() == 1)
        solr.delete_doc_by_id(test_config['SOLR_COLLECTION'], '*')

    def test_index_multiproc(self):
        index = IndexQ(test_config['indexqbase'], 'testq')
        solr = SolrClient(test_config['SOLR_SERVER'],
                          devel=True,
                          auth=test_config['SOLR_CREDENTIALS'])
        solr.delete_doc_by_id(test_config['SOLR_COLLECTION'], '*')
        buff = []
        files = []
        for doc in self.docs:
            files.append(index.add(doc, finalize=True))
        index.index(solr, test_config['SOLR_COLLECTION'], threads=10)
        solr.commit(test_config['SOLR_COLLECTION'], openSearcher=True)
        for doc in self.docs:
            res = solr.query(test_config['SOLR_COLLECTION'],
                             {'q': 'id:{}'.format(doc['id'])})
            self.assertTrue(res.get_results_count() == 1)

    def test_index_bad_send_method(self):
        index = IndexQ(test_config['indexqbase'], 'testq')
        solr = SolrClient(test_config['SOLR_SERVER'],
                          devel=True, auth=test_config['SOLR_CREDENTIALS'])
        with self.assertRaises(AttributeError):
            index.index(solr,
                        test_config['SOLR_COLLECTION'],
                        send_method='Doesnt exist')

    def test_index_bad_data(self):
        index = IndexQ(test_config['indexqbase'], 'testq')
        solr = SolrClient(test_config['SOLR_SERVER'],
                          devel=True,
                          auth=test_config['SOLR_CREDENTIALS'])
        if index._is_locked():
            index._unlock()
        self.assertEqual(index.get_all_as_list(), [])
        solr.delete_doc_by_id(test_config['SOLR_COLLECTION'], '*')
        todo_file = index.add({'date': 'asd'}, finalize=True)
        self.assertEqual(index.get_all_as_list()[0], todo_file)
        with self.assertRaises(SolrError):
            index.index(solr, test_config['SOLR_COLLECTION'])
        self.assertEqual(index.get_all_as_list()[0], todo_file)
        self.assertFalse(index._is_locked())

    def test_index_dynamic_collections_basic_1(self):
        index = IndexQ(test_config['indexqbase'], 'testq')
        solr = SolrClient(test_config['SOLR_SERVER'],
                          devel=True,
                          auth=test_config['SOLR_CREDENTIALS'])
        if index._is_locked():
            index._unlock()
        self.assertEqual(index.get_all_as_list(), [])

        # Set up mock for indexing
        temp = {}

        def mock(temp, coll, docs):
            temp[coll] = docs
            return True

        todo_file = index.add([{'type': '1', 'data': '1'},
                               {'type': '1', 'data': '2'},
                               {'type': '1', 'data': '3'},
                               {'type': '2', 'data': '4'},
                               {'type': '3', 'data': '5'},
                               ], finalize=True)
        runner_wrap = index._wrap_dynamic(partial(mock, temp),
                                          lambda x: x['type'],
                                          todo_file)
        self.assertTrue(runner_wrap)
        self.assertEqual(json.loads(temp['3']), [{"data": "5", "type": "3"}])
        self.assertEqual(json.loads(temp['2']), [{'type': '2', 'data': '4'}])
        self.assertEqual(sorted(json.loads(temp['1']), key=lambda x: x['data']),
                         sorted([{'type': '1', 'data': '1'},
                                 {'type': '1', 'data': '2'},
                                 {'type': '1', 'data': '3'}],
                                key=lambda x: x['data']))
        self.assertFalse(index.get_all_as_list())  # Make sure item is completed

    def test_index_dynamic_collections_func_basic_error_1(self):
        index = IndexQ(test_config['indexqbase'], 'testq')
        solr = SolrClient(test_config['SOLR_SERVER'],
                          devel=True,
                          auth=test_config['SOLR_CREDENTIALS'])
        if index._is_locked():
            index._unlock()
        self.assertEqual(index.get_all_as_list(), [])

        # Set up mock for indexing
        temp = {}

        def mock(temp, coll, docs):
            temp[coll] = docs

        todo_file = index.add([{'type': '1', 'data': '1'},
                               {'type': '1', 'data': '2'},
                               {'type': '1', 'data': '3'},
                               {'type': '2', 'data': '4'},
                               {'type': '3', 'data': '5'},
                               ], finalize=True)
        with self.assertRaises(KeyError):
            index._wrap_dynamic(partial(mock, temp),
                                lambda x: x['asdasdasd'],
                                todo_file)

    def test_index_dynamic_collections_indexing_error(self):
        index = IndexQ(test_config['indexqbase'], 'testq')
        solr = SolrClient(test_config['SOLR_SERVER'],
                          devel=True,
                          auth=test_config['SOLR_CREDENTIALS'])
        if index._is_locked():
            index._unlock()
        self.assertEqual(index.get_all_as_list(), [])

        # Set up mock for indexing
        temp = {}

        def mock(temp, coll, docs):
            raise KeyError()

        todo_file = index.add([{'type': '1', 'data': '1'},
                               {'type': '1', 'data': '2'},
                               {'type': '1', 'data': '3'},
                               {'type': '2', 'data': '4'},
                               {'type': '3', 'data': '5'},
                               ], finalize=True)
        runner_wrap = index._wrap_dynamic(partial(mock, temp),
                                          lambda x: x['type'],
                                          todo_file)
        self.assertFalse(runner_wrap)

    def test_index_dynamic_collections_indexing_error_partial(self):
        index = IndexQ(test_config['indexqbase'], 'testq')
        solr = SolrClient(test_config['SOLR_SERVER'],
                          devel=True,
                          auth=test_config['SOLR_CREDENTIALS'])
        if index._is_locked():
            index._unlock()
        self.assertEqual(index.get_all_as_list(), [])

        # Set up mock for indexing
        temp = {}

        def mock(temp, coll, docs):
            if json.loads(docs)[0]['type'] == '1':
                raise KeyError()
            else:
                temp[coll] = docs
                return True

        todo_file = index.add([{'type': '1', 'data': '1'},
                               {'type': '1', 'data': '2'},
                               {'type': '1', 'data': '3'},
                               {'type': '2', 'data': '4'},
                               {'type': '3', 'data': '5'},
                               ], finalize=True)
        runner_wrap = index._wrap_dynamic(partial(mock, temp),
                                          lambda x: x['type'],
                                          todo_file)
        self.assertFalse(runner_wrap)

    def test_thread_pool_low(self):
        '''
        Index data using multiple threads.
        Verity that each thread
        '''
        docs = self.rand_docs.get_docs(5)
        threads = 5
        index = IndexQ(test_config['indexqbase'], 'testq', size=1)
        with ThreadPool(threads) as p:
            p.map(index.add, docs)
        self.check_file_contents(index.add(finalize=True), docs)

    def test_thread_pool_mid(self):
        '''
        Index data using multiple threads.
        Verity that each thread
        '''
        docs = self.rand_docs.get_docs(5000)
        threads = 5
        index = IndexQ(test_config['indexqbase'], 'testq', size=1)
        with ThreadPool(threads) as p:
            p.map(index.add, docs)
        index.add(finalize=True)
        d = index.get_all_json_from_indexq()
        self.assertEqual(sorted(d, key=lambda x: x['id']), sorted(docs, key=lambda x: x['id']))

    def test_thread_pool_high(self):
        '''
        Index data using multiple threads.
        Verity that each thread
        '''
        docs = self.rand_docs.get_docs(25000)
        index = IndexQ(test_config['indexqbase'],
                       'testq',
                       size=.1,
                       devel=True)
        for dir in ['_todo_dir', '_done_dir']:
            [os.remove(x) for x in index.get_all_as_list(dir=dir)]
        threads = 25

        with ThreadPool(threads) as p:
            p.map(index.add, docs)
        index.add(finalize=True)
        d = index.get_all_json_from_indexq()
        self.assertEqual(len(d), len(docs))
        self.assertEqual(sorted(d, key=lambda x: x['id']),
                         sorted(docs, key=lambda x: x['id']))

    def test_add_callback_no_size(self):
        docs = self.rand_docs.get_docs(5)
        index = IndexQ(test_config['indexqbase'], 'testq')
        temp = []

        def cb(path):
            temp.append(path)

        t = index.add(docs[0], callback=cb)
        self.assertTrue(t in temp)

    def test_add_callback_with_size(self):
        docs = self.rand_docs.get_docs(5)
        index = IndexQ(test_config['indexqbase'], 'testq', size=1)
        temp = []

        def cb(path):
            temp.append(path)

        t = index.add(docs[0], callback=cb)
        t = index.add(docs[1], callback=cb, finalize=True)
        self.assertTrue(t in temp)

    def test_get_multi_q1(self):
        docs = self.rand_docs.get_docs(5000)
        log = logging.getLogger()
        index = IndexQ(test_config['indexqbase'], 'testq', size=1, log=log)
        q = index.get_multi_q()
        for item in docs:
            q.put(item)
        q.put('STOP')
        index.join_indexer()
        self.assertEqual(docs, index.get_all_json_from_indexq())

    def test_get_multi_q2(self):
        log = logging.getLogger()
        index = IndexQ(test_config['indexqbase'], 'testq', size=1, log=log)
        q = index.get_multi_q()
        docs = self.rand_docs.get_docs(50000)
        for item in docs:
            q.put(item)
        q.put('STOP')
        index.join_indexer()
        self.assertEqual(docs, index.get_all_json_from_indexq())

    def test_get_multi_q3(self):
        log = logging.getLogger()
        index = IndexQ(test_config['indexqbase'], 'testq', size=1, log=log)
        q = index.get_multi_q()
        docs = self.rand_docs.get_docs(5000)
        docs2 = self.rand_docs.get_docs(5000)
        for item in docs + ['STOP'] + docs2:
            q.put(item)
        index.join_indexer()
        self.assertEqual(docs + docs2, index.get_all_json_from_indexq())

    def test_get_multi_with_sentinel(self):
        log = logging.getLogger()
        index = IndexQ(test_config['indexqbase'], 'testq', size=1, log=log)
        q = index.get_multi_q(sentinel='BLAH')
        docs = self.rand_docs.get_docs(5000)
        docs2 = self.rand_docs.get_docs(5000)
        for item in docs + ['BLAH'] + docs2:
            q.put(item)
        index.join_indexer()
        self.assertEqual(docs + docs2, index.get_all_json_from_indexq())

    def test_complete_dir_rotate(self):
        log = logging.getLogger()
        rotate_func = lambda: '{}/{}/{}'.format(dt.now().year, dt.now().month, dt.now().day)
        index = IndexQ(test_config['indexqbase'], 'testq', size=1, log=log,
                       rotate_complete=rotate_func)
        dir_set = rotate_func()
        docs = self.rand_docs.get_docs(69)
        for item in self.docs[1:10]:
            index.add(item, finalize=True)
        files = []
        for item in index.get_all_as_list():
            files.append(index.complete(item))
        [self.assertTrue(os.path.exists(x)) for x in files]

    def test_complete_compress_basic(self):
        log = logging.getLogger()
        index = IndexQ(test_config['indexqbase'], 'testq', size=1, log=log,
                       compress=True)
        for item in self.docs[1:10]:
            index.add(item, finalize=True)
        files = []
        for item in index.get_all_as_list():
            files.append(index.complete(item))
        [self.assertTrue(os.path.exists(x)) for x in files]

    def test_complete_compress_basic_re_indexing(self):
        log = logging.getLogger()
        solr = SolrClient(test_config['SOLR_SERVER'],
                          devel=True,
                          auth=test_config['SOLR_CREDENTIALS'])
        index = IndexQ(test_config['indexqbase'], 'testq', size=1, log=log,
                       compress=True)
        solr.delete_doc_by_id(test_config['SOLR_COLLECTION'], '*')
        for item in self.docs[1:10]:
            index.add(item, finalize=True)
        index.index(solr, test_config['SOLR_COLLECTION'])
        # At this point items are indexed and are moved into the done directory
        # Lets re-index them to make sure all json got properly encoded
        files = index.get_all_as_list('_done_dir')
        for f in index.get_all_as_list('_done_dir'):
            shutil.move(f, index._todo_dir)
        index.index(solr, test_config['SOLR_COLLECTION'])
        self.assertEqual(files, index.get_all_as_list('_done_dir'))
