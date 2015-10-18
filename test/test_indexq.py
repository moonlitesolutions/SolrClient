import unittest
import gzip
import logging
import json
import os
from time import sleep
import random
from SolrClient import SolrClient, IndexQ
from .RandomTestData import RandomTestData

test_config = {
    'indexqbase':os.getcwd(),
}

class TestIndexQ(unittest.TestCase):
    
    @classmethod
    def setUpClass(self):
        self.rand_docs = RandomTestData()
        self.docs = self.rand_docs.get_docs(50)
        self.devel = True
        if self.devel:
            logging.basicConfig(level=logging.DEBUG,format='%(asctime)s [%(levelname)s] (%(process)d) (%(threadName)-10s) [%(name)s] %(message)s')

    def setUp(self):
        index = IndexQ(test_config['indexqbase'],'testq')
        try:
            [os.remove(x) for x in [index.get_all_items(dir=y) for y in ['_todo_dir','_done_dir']]]
        except:
            pass
            
    def check_file_contents(self,file_path,real_data):
        if os.path.isfile(file_path):
            if file_path.endswith('.gz'):
                f = gzip.open(file_path,'rt',encoding='utf-8')
                f_data = json.load(f)
            else:
                f = open(file_path)
                f_data = json.load(f)
            f.close()
        [self.assertEqual(f_data[x],real_data[x]) for x in range(len(real_data)) ] 
    
    @unittest.skip("Don't test remote indexing in travis")
    def test_add_bad_list(self):
        index = IndexQ(test_config['indexqbase'],'testq')
        with self.assertRaises(ValueError):
            index.add([{},{},[],{}])
    @unittest.skip("Don't test remote indexing in travis")
    def test_add_string(self):
        index = IndexQ(test_config['indexqbase'], 'testq')
        string_test = 'asd'
        doc = index.add(string_test)
        with open(doc) as f:
            doc_data = f.read()
        self.assertEqual(string_test,doc_data)
    @unittest.skip("Don't test remote indexing in travis")
    def test_add_int(self):
        index = IndexQ(test_config['indexqbase'],'testq')
        with self.assertRaises(ValueError):
            index.add(1)
    @unittest.skip("Don't test remote indexing in travis")
    def test_add_good_dict_zero_size(self):
        index = IndexQ(test_config['indexqbase'],'testq')
        doc = index.add(self.docs[0])
        #Sending docs as list because that is how json is stored
        self.check_file_contents(doc,[self.docs[0]])
    @unittest.skip("Don't test remote indexing in travis")
    def test_add_good_list_zero_size(self):
        index = IndexQ(test_config['indexqbase'], 'testq')
        doc = index.add(self.docs[0:20])
        self.check_file_contents(doc,self.docs[0:20])
    @unittest.skip("Don't test remote indexing in travis")
    def test_add_good_list_zero_size_compressed(self):
        index = IndexQ(test_config['indexqbase'], 'testq', compress=True)
        doc = index.add(self.docs[0:20])
        self.check_file_contents(doc,self.docs[0:20])
    @unittest.skip("Don't test remote indexing in travis")
    def test_add_good_dict_zero_size(self):
        index = IndexQ(test_config['indexqbase'], 'testq', compress=True)
        doc = index.add(self.docs[0])
        #Sending docs as list because that is how json is stored
        self.check_file_contents(doc,[self.docs[0]])
    @unittest.skip("Don't test remote indexing in travis")
    def test_buffer_list_1m(self):
        size = 1
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        while True:
            doc = index.add(self.docs)
            [buff.append(x) for x in self.docs]
            if type(doc) is str:
                break
        self.check_file_contents(doc,buff)
        self.assertLessEqual(os.path.getsize(doc),size*1000000)
        self.assertGreaterEqual(os.path.getsize(doc), size*1000000*.90)
        os.remove(doc)
    @unittest.skip("Don't test remote indexing in travis")
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
        self.check_file_contents(doc,buff)
        self.assertLessEqual(os.path.getsize(doc), size * 1000000)
        self.assertGreaterEqual(os.path.getsize(doc), size * 1000000 * .90)
        os.remove(doc)
    @unittest.skip("Don't test remote indexing in travis")
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
        self.check_file_contents(doc,buff)
        self.assertLessEqual(os.path.getsize(doc), size * 1000000)
        self.assertGreaterEqual(os.path.getsize(doc), size * 1000000 * .90)
        os.remove(doc)
    @unittest.skip("Don't test remote indexing in travis")
    def test_buffer_list_25m(self):
        size = 25
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        while True:
            doc = index.add(self.docs)
            [buff.append(x) for x in self.docs]
            if type(doc) is str:
                break
        self.check_file_contents(doc,buff)
        self.assertLessEqual(os.path.getsize(doc),size*1000000)
        self.assertGreaterEqual(os.path.getsize(doc), size*1000000*.90)
        os.remove(doc)
        
    @unittest.skip("Don't test remote indexing in travis")
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
        self.check_file_contents(doc,buff)
        self.assertLessEqual(os.path.getsize(doc), size * 1000000)
        self.assertGreaterEqual(os.path.getsize(doc), size * 1000000 * .90)
        os.remove(doc)
        
    @unittest.skip("Don't test remote indexing in travis")
    def test_buffer_list_75m(self):
        size = 75
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        while True:
            doc = index.add(self.docs)
            [buff.append(x) for x in self.docs]
            if type(doc) is str:
                break
        self.check_file_contents(doc,buff)
        self.assertLessEqual(os.path.getsize(doc),size*1000000)
        self.assertGreaterEqual(os.path.getsize(doc), size*1000000*.90)
        os.remove(doc)
    
    @unittest.skip("Don't test remote indexing in travis")
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
        self.check_file_contents(doc,buff)
        os.remove(doc)
    
    def test_by_get_all_compressed(self):
        size = 1
        files = 2
        index = IndexQ(test_config['indexqbase'], 'testq', size=size,compress=True)
        buff = []
        docs = []
        for _ in range(files):
            doc = index.add(self.docs,finalize=True)
            docs.append(doc)
        index = IndexQ(test_config['indexqbase'], 'testq', mode='out')
        indexdocs = index.get_all_as_list()
        self.assertEqual(docs,indexdocs)
        [os.remove(doc) for doc in docs]

    def test_by_get_all_no_compressed(self):
        size = 1
        files = 2
        index = IndexQ(test_config['indexqbase'], 'testq', size=size,compress=False)
        buff = []
        docs = []
        for _ in range(files):
            doc = index.add(self.docs,finalize=True)
            docs.append(doc)
        index = IndexQ(test_config['indexqbase'], 'testq', mode='out')
        indexdocs = index.get_all_as_list()
        self.assertEqual(docs,indexdocs)
        [os.remove(doc) for doc in docs]
    
    def test_by_get_all_default_compression(self):
        size = 1
        files = 2
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        docs = []
        for _ in range(files):
            doc = index.add(self.docs,finalize=True)
            docs.append(doc)
        index = IndexQ(test_config['indexqbase'], 'testq', mode='out')
        indexdocs = index.get_all_as_list()
        self.assertEqual(docs,indexdocs)
        [os.remove(doc) for doc in docs]
    
    def test_dequeue(self):
        size = 1
        files = 2
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        docs = []
        for _ in range(files):
            doc = index.add(self.docs,finalize=True)
            docs.append(doc)
        index = IndexQ(test_config['indexqbase'], 'testq', mode='out')
        indexdocs = []
        for x in index.get_todo_items():
            indexdocs.append(x)
        self.assertEqual(docs,indexdocs)
        [os.remove(doc) for doc in docs]
    
    def test_dequeue_100(self):
        size = 1
        files = 100
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        docs = []
        for _ in range(files):
            doc = index.add(self.docs,finalize=True)
            docs.append(doc)
        index = IndexQ(test_config['indexqbase'], 'testq', mode='out')
        indexdocs = []
        for x in index.get_todo_items():
            indexdocs.append(x)
        self.assertEqual(docs,indexdocs)
        [os.remove(doc) for doc in docs]


    def test_dequeue_and_complete_no_compression_5(self):
        size = 1
        files = 5
        index = IndexQ(test_config['indexqbase'], 'testq', size=size)
        buff = []
        docs = []
        for _ in range(files):
            doc = index.add(self.docs,finalize=True)
            docs.append(doc)
        index = IndexQ(test_config['indexqbase'], 'testq', mode='out',compress=False)
        indexdocs = []
        for x in index.get_todo_items():
            indexdocs.append(x)
            index.complete(x)
        self.assertEqual(docs,indexdocs)
        finaldocnames = [os.path.split(x)[-1] for x in indexdocs]
        donefilepaths = [os.path.join(index._done_dir,x) for x in finaldocnames]
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
            doc = index.add(self.docs,finalize=True)
            docs.append(doc)
        
        index = IndexQ(test_config['indexqbase'], 'testq', mode='out',devel=True)
        x = index.get_todo_items()
        self.assertTrue(os.path.exists(index._lck))
        with self.assertRaises(RuntimeError) as a:
            new_index = IndexQ(test_config['indexqbase'], 'testq', mode='out')
            y = new_index.get_todo_items()
        #Run through the items to finish the de-queue
        [index.complete(i) for i in x]
        self.assertFalse(os.path.exists(index._lck))
        
    #Indexing TEst
    #
    #def test_indexing(self):
        