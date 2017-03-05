import unittest
import logging
import json
import itertools
from time import sleep
from SolrClient import SolrClient
from .test_config import test_config
from .RandomTestData import RandomTestData
#logging.basicConfig(level=logging.DEBUG,format='%(asctime)s [%(levelname)s] (%(process)d) (%(threadName)-10s) [%(name)s] %(message)s')
logging.disable(logging.CRITICAL)

class ClientTestQuery(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.solr = SolrClient(test_config['SOLR_SERVER'][0],devel=True,auth=test_config['SOLR_CREDENTIALS'])
        self.rand_docs = RandomTestData()
        self.docs = self.rand_docs.get_docs(50)
        self.solr.delete_doc_by_id(test_config['SOLR_COLLECTION'],'*')

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

        #Index Some data
        self.solr.index_json(test_config['SOLR_COLLECTION'],json.dumps(self.docs))
        self.solr.commit(test_config['SOLR_COLLECTION'],openSearcher=True)

    def test_basic_query(self):
        r = self.solr.query(test_config['SOLR_COLLECTION'],{'q':'*:*'})
        self.assertEqual(r.get_num_found(),len(self.docs))

    def test_facet(self):
        r = self.solr.query(test_config['SOLR_COLLECTION'],{
            'q':'*:*',
            'facet':'true',
            'facet.field':'facet_test',
        })
        local_facets = {}
        for doc in self.docs:
            try:
                local_facets[doc['facet_test']] +=1
            except:
                local_facets[doc['facet_test']] = 1
        try:
            self.assertDictEqual(local_facets,r.get_facets()['facet_test'])
        except Exception as e:
            logging.info("local")
            logging.info(local_facets)
            logging.info("facets")
            logging.info(r.get_facets())
            raise

    def test_facet_with_fq(self):
        r = self.solr.query(test_config['SOLR_COLLECTION'],{
            'q':'*:*',
            'facet':True,
            'facet.field':'facet_test',
        })
        first_facet_field = list(r.get_facets()['facet_test'].keys())[0]
        first_facet_field_count = r.get_facets()['facet_test'][first_facet_field]
        r = self.solr.query(test_config['SOLR_COLLECTION'],{
            'q':'*:*',
            'facet':True,
            'facet.field':'facet_test',
            'fq':'facet_test:{}'.format(first_facet_field)
        })
        self.assertEqual(r.get_num_found(),first_facet_field_count)

    def test_facet_range(self):
        res = self.solr.query(test_config['SOLR_COLLECTION'],{
            'q':'*:*',
            'facet':True,
            'facet.range':'price',
            'facet.range.start':0,
            'facet.range.end':100,
            'facet.range.gap':10
            })

        prices = [doc['price'] for doc in self.docs]
        div = lambda x: str(x//10 * 10)
        out = {}
        for k,g in itertools.groupby(sorted(prices),div):
            out[k] = len(list(g)) or 0
        self.assertDictEqual(out,res.get_facets_ranges()['price'])

    def test_facet_pivot(self):
        res = self.solr.query(test_config['SOLR_COLLECTION'],{
            'q':'*:*',
            'facet':True,
            'facet.pivot':['facet_test,price','facet_test,id']
        })
        out = {}
        for doc in self.docs:
            if doc['facet_test'] not in out:
                out[doc['facet_test']] = {}
            if doc['price'] not in out[doc['facet_test']]:
                out[doc['facet_test']][doc['price']]=1
            else:
                out[doc['facet_test']][doc['price']]+=1
        self.assertDictEqual(out,res.get_facet_pivot()['facet_test,price'])


    def test_get_field_values_as_list(self):
        res = self.solr.query(test_config['SOLR_COLLECTION'],{
            'q':'*:*',
            })
        results = res.get_field_values_as_list('product_name_exact')
        docs = res.docs
        temp = []
        for doc in docs:
            if 'product_name_exact' in doc:
                temp.append(doc['product_name_exact'])
        self.assertEqual(results,temp)


    def test_get_facet_values_as_list(self):
        r = self.solr.query(test_config['SOLR_COLLECTION'],{
            'q':'*:*',
            'facet':'true',
            'facet.limit': -1,
            'facet.field':'facet_test',
        })
        self.assertEqual(
            sorted(r.data['facet_counts']['facet_fields']['facet_test'][1::2]),
            sorted(r.get_facet_values_as_list('facet_test'))
        )

    def test_grouped_count_1(self):
        '''
        Get a dict of grouped docs
        '''
        r = self.solr.query(test_config['SOLR_COLLECTION'],{
            'q': '*:*',
            'group': True,
            'group.field': 'id',
            'group.ngroups': True,
        })
        self.assertEqual(r.get_ngroups(), 50)
        self.assertEqual(r.get_ngroups('id'), 50)


    def test_grouped_docs(self):
        '''
        Get a dict of grouped docs
        '''
        r = self.solr.query(test_config['SOLR_COLLECTION'],{
            'q': '*:*',
            'group': True,
            'group.field': 'id',
            'group.ngroups': True,
        })
        self.assertEqual(len(r.docs), 10)
        self.assertTrue('doclist' in r.docs[0])

    def test_grouped_docs(self):
        '''
        Get a dict of grouped docs
        '''
        r = self.solr.query(test_config['SOLR_COLLECTION'],{
            'q': '*:*',
            'group': True,
            'group.field': 'id',
            'group.ngroups': True,
        })
        self.assertEqual(len(r.docs), 10)
        self.assertTrue('doclist' in r.docs[0])


    def test_flat_groups(self):
        '''
        Get a dict of grouped docs
        '''
        r = self.solr.query(test_config['SOLR_COLLECTION'],{
            'q': '*:*',
            'group': True,
            'group.field': 'id'
             })
        flats = r.get_flat_groups()
        self.assertEqual(len(flats), 10)
        self.assertTrue('date' in flats[0])

    def test_json_facet(self):
        '''
        Get a dict of grouped docs
        '''
        #Just lazy getting a new response object
        r = self.solr.query(test_config['SOLR_COLLECTION'],{ 'q': '*:*'})

        a = r.get_jsonfacet_counts_as_dict('test',
                {'count': 50,
                  'test': {'buckets': [{'count': 10,
                     'pr': {'buckets': [
                       {'count': 2, 'unique': 1, 'val': 79},
                       {'count': 1, 'unique': 1, 'val': 9}]},
                     'pr_sum': 639.0,
                     'val': 'consectetur'},
                    {'count': 8,
                     'pr': {'buckets': [
                       {'count': 1, 'unique': 1, 'val': 9},
                       {'count': 1, 'unique': 1, 'val': 31},
                       {'count': 1, 'unique': 1, 'val': 33},]},
                     'pr_sum': 420.0,
                     'val': 'auctor'},
                    {'count': 8,
                     'pr': {'buckets': [
                       {'count': 2, 'unique': 1, 'val': 94},
                       {'count': 1, 'unique': 1, 'val': 25},
                       ]},
                     'pr_sum': 501.0,
                     'val': 'nulla'}]}})

        b =  {'test': {'auctor': {'count': 8,
                     'pr': {9: {'count': 1, 'unique': 1},
                            31: {'count': 1, 'unique': 1},
                            33: {'count': 1, 'unique': 1}},
                     'pr_sum': 420.0},
          'consectetur': {'count': 10,
                          'pr': {9: {'count': 1, 'unique': 1},
                                 79: {'count': 2, 'unique': 1}},
                          'pr_sum': 639.0},
          'nulla': {'count': 8,
                    'pr': {25: {'count': 1, 'unique': 1},
                           94: {'count': 2, 'unique': 1}},
                    'pr_sum': 501.0}}}

        self.assertEqual(a, b)
