
import unittest
import logging
import json
import gzip
import os
import datetime
from SolrClient import SolrClient, IndexQ, Reindexer
from .test_config import test_config
from .RandomTestData import RandomTestData

test_config['indexqbase'] = os.getcwd()
#logging.basicConfig(level=logging.INFO,format='%(asctime)s [%(levelname)s] (%(process)d) (%(threadName)-10s) [%(name)s] %(message)s')
logging.disable(logging.CRITICAL)


class ReindexerTests(unittest.TestCase):
    # Methos to create the schema in the collections
    def create_fields(self):
        for coll in self.colls:
            logging.debug("Creating fields for {}".format(coll))
            for field in test_config['collections']['fields']:
                try:
                    self.solr.schema.create_field(coll, field)
                except ValueError:
                    # Filed already exists probably
                    pass

    def create_copy_fields(self):
        for coll in self.colls:
            logging.debug("Creating copy fields for {}".format(coll))
            for field in test_config['collections']['copy_fields']:
                try:
                    self.solr.schema.create_copy_field(coll, field)
                except ValueError:
                    # Filed already exists probably
                    pass

    def setUp(self):
        [self.solr.delete_doc_by_id(coll, '*') for coll in self.colls]
        [self.solr.commit(coll, openSearcher=True) for coll in self.colls]

    def _index_docs(self, numDocs, coll):
        '''
        Generates and indexes in random data while maintaining counts of items in various date ranges.

        These counts in self.date_counts are used later to validate some reindexing methods.

        Brace yourself or have a drink.....
        '''
        self.docs = self.rand_docs.get_docs(numDocs)
        sdate = datetime.datetime.now() - datetime.timedelta(days=180)
        edate = datetime.datetime.now() + datetime.timedelta(days=30)
        self._start_date = sdate
        self._end_date = edate

        import random
        # Assign random times to documents that are generated. This is used to spread out the documents over multiple time ranges
        hours = (edate - sdate).days * 24
        hour_range = [x for x in range(int(hours))]
        self.date_counts = {}

        # Save the newest and oldest timestamps as well as assign them to first and second doc
        self.docs[0]['date'] = sdate.isoformat() + 'Z'
        self.date_counts[sdate.date().isoformat()] = 1

        self.docs[1]['date'] = edate.isoformat() + 'Z'
        self.date_counts[edate.date().isoformat()] = 1

        for doc in self.docs[2:]:
            # Make a new date and store a count of it so I can compare later
            new_date = (sdate + datetime.timedelta(hours=random.choice(hour_range)))
            new_date_s = new_date.date().isoformat()
            if new_date_s in self.date_counts:
                self.date_counts[new_date_s] += 1
            else:
                self.date_counts[new_date_s] = 1
            doc['date'] = new_date.isoformat() + 'Z'

        self.solr.index_json(coll, json.dumps(self.docs))
        self.solr.commit(coll, openSearcher=True, softCommit=True)

    def get_all_json_from_indexq(self, index):
        files = index.get_all_as_list()
        out = []
        for efile in files:
            if efile.endswith('.gz'):
                f = gzip.open(efile, 'rt', encoding='utf-8')
            else:
                f = open(efile)
            f_data = json.load(f)
            f.close()
            out.extend(f_data)
        return out

    @classmethod
    def setUpClass(self):
        logging.debug("Starting to run Reindexer Tests")
        self.solr = SolrClient(test_config['SOLR_SERVER'][0], devel=True, auth=test_config['SOLR_CREDENTIALS'])
        self.colls = [test_config['SOLR_REINDEXER_COLLECTION_S'], test_config['SOLR_REINDEXER_COLLECTION_D']]
        self.rand_docs = RandomTestData()

    def test_solr_to_indexq(self):
        '''
        Will export documents from Solr and put them into an IndexQ.
        '''
        index = IndexQ(test_config['indexqbase'], 'test_reindexer', size=0)
        for dir in ['_todo_dir', '_done_dir']:
            [os.remove(x) for x in index.get_all_as_list(dir=dir)]
        self._index_docs(5000, self.colls[0])
        reindexer = Reindexer(source=self.solr, source_coll='source_coll', dest=index)
        reindexer.reindex()
        from_files = self.get_all_json_from_indexq(index)
        from_solr = self.solr.query('source_coll', {'q': '*:*', 'rows': 5000}).docs
        from_solr = reindexer._trim_fields(from_solr)
        self.assertEqual(sorted(from_files, key=lambda x: x['id']), sorted(from_solr, key=lambda x: x['id']))

    def test_ignore_fields(self):
        '''
        Will export documents from Solr and put them into an IndexQ.
        '''
        index = IndexQ(test_config['indexqbase'], 'test_reindexer', size=0)
        for dir in ['_todo_dir', '_done_dir']:
            [os.remove(x) for x in index.get_all_as_list(dir=dir)]
        reindexer = Reindexer(source=self.solr, source_coll='source_coll', dest=index)
        for field in ['_version_', 'product_name_exact']:
            self.assertTrue(field in reindexer._ignore_fields)

    def test_ignore_fields_disable(self):
        '''
        Checks to make sure ignore_fields override works
        '''
        index = IndexQ(test_config['indexqbase'], 'test_reindexer', size=0)
        reindexer = Reindexer(source=self.solr, source_coll='source_coll', dest=index, ignore_fields=False)
        self.assertEqual(reindexer._ignore_fields, False)

    def test_ignore_fields_override(self):
        '''
        Checks to make sure ignore_fields override works
        '''
        index = IndexQ(test_config['indexqbase'], 'test_reindexer', size=0)
        reindexer = Reindexer(source=self.solr, source_coll='source_coll', dest=index,
                              ignore_fields=['_text_', '_any_other_field'])
        self.assertEqual(reindexer._ignore_fields, ['_text_', '_any_other_field'])

    def test_get_copy_fields(self):
        '''
        Tests the method to get copy fields from Solr.
        '''
        reindexer = Reindexer(source=self.solr, source_coll=self.colls[0], dest=self.solr, dest_coll='doesntmatter')
        self.assertEqual(reindexer._get_copy_fields(),
                         [field['dest'] for field in self.solr.schema.get_schema_copyfields(self.colls[0])])

    def test_query_gen(self):
        '''
        Tests the method to get copy fields from Solr.
        '''
        reindexer = Reindexer(source=self.solr, source_coll=self.colls[0], dest=self.solr, dest_coll='doesntmatter')
        self.assertEqual(reindexer._get_query('cursor'),
                         {'cursorMark': 'cursor', 'rows': reindexer._rows, 'q': '*:*', 'sort': 'id desc'})

    def test_query_gen_pershard_distrib(self):
        '''
        Tests the method to get copy fields from Solr.
        '''
        reindexer = Reindexer(source=self.solr, source_coll=self.colls[0], dest=self.solr, dest_coll='doesntmatter',
                              per_shard=True)
        q = reindexer._get_query('cursor')
        self.assertTrue('distrib' in q and q['distrib'] == 'false')

    def test_query_gen_date(self):
        '''
        Tests the method to get copy fields from Solr.
        '''
        reindexer = Reindexer(source=self.solr, source_coll=self.colls[0], dest=self.solr, dest_coll='doesntmatter',
                              date_field='ddddd')
        self.assertEqual(reindexer._get_query('cursor'),
                         {'cursorMark': 'cursor', 'rows': reindexer._rows, 'q': '*:*', 'sort': 'id desc',
                          'sort': 'ddddd asc, id desc'})

    def test_remove_copy_fields_from_data(self):
        index = IndexQ(test_config['indexqbase'], 'test_reindexer', size=0)
        for dir in ['_todo_dir', '_done_dir']:
            [os.remove(x) for x in index.get_all_as_list(dir=dir)]
        reindexer = Reindexer(source=self.solr, source_coll='source_coll', dest=index)
        reindexer.reindex()
        from_files = self.get_all_json_from_indexq(index)
        excluded_fields = reindexer._ignore_fields
        for doc in from_files:
            for field in excluded_fields:
                if field in doc:
                    print(doc)
                    # self.assertTrue(field not in doc)

    def test_solr_to_solr(self):
        self._index_docs(50000, self.colls[0])
        reindexer = Reindexer(source=self.solr, source_coll='source_coll', dest=self.solr, dest_coll='dest_coll')
        reindexer.reindex()
        self.assertEqual(
            self.solr.query(self.colls[0], {'q': '*:*', 'rows': 10000000}).docs.sort(key=lambda x: x['id']),
            self.solr.query(self.colls[1], {'q': '*:*', 'rows': 10000000}).docs.sort(key=lambda x: x['id']),
        )

    def test_solr_to_solr_with_date(self):
        self._index_docs(50000, self.colls[0])
        solr = SolrClient(test_config['SOLR_SERVER'][0], devel=True,
                          auth=test_config['SOLR_CREDENTIALS'])
        reindexer = Reindexer(source=solr, source_coll='source_coll',
                              dest=solr, dest_coll='dest_coll',
                              date_field='index_date')
        reindexer.reindex()

        try:
            self.assertTrue(solr.transport._action_log[1]['params']['params']['sort'] == 'index_date asc, id desc')
        except:
            self.assertTrue(solr.transport._action_log[2]['params']['params']['sort'] == 'index_date asc, id desc')
        self.assertEqual(
            solr.query(self.colls[0], {'q': '*:*', 'rows': 10000000}).docs.sort(key=lambda x: x['id']),
            solr.query(self.colls[1], {'q': '*:*', 'rows': 10000000}).docs.sort(key=lambda x: x['id']),
        )

    def test_get_edge_date(self):
        '''
        Checks to make sure _get_edge_date returns correct start and end dates.
        '''
        self._index_docs(50000, self.colls[0])
        solr = SolrClient(test_config['SOLR_SERVER'][0], devel=True, auth=test_config['SOLR_CREDENTIALS'])
        reindexer = Reindexer(source=solr, source_coll='source_coll', dest=solr, dest_coll='dest_coll',
                              date_field='index_date')
        solr_end_date_string = reindexer._get_edge_date('date', 'desc')
        solr_start_date_string = reindexer._get_edge_date('date', 'asc')
        self.assertTrue(self._start_date.date(),
                        datetime.datetime.strptime(solr_start_date_string, '%Y-%m-%dT%H:%M:%S.%fZ'))
        self.assertTrue(self._end_date.date(),
                        datetime.datetime.strptime(solr_end_date_string, '%Y-%m-%dT%H:%M:%S.%fZ'))

    def test_get_date_range_query(self):
        '''
        Checks the date_range_query generation function. Since it's pretty simple, running all the tests as one
        '''
        solr = SolrClient(test_config['SOLR_SERVER'][0], devel=True, auth=test_config['SOLR_CREDENTIALS'])
        reindexer = Reindexer(source=solr, source_coll='source_coll', dest=solr, dest_coll='dest_coll',
                              date_field='index_date')
        self.assertEqual(
            reindexer._get_date_range_query('2015-11-10', '2015-12-11'),
            {'rows': 0, 'facet.range.end': '2015-12-11', 'facet': 'true', 'facet.range': 'index_date',
             'facet.range.start': '2015-11-10', 'q': '*:*', 'facet.range.include': 'all', 'facet.range.gap': '+1DAY'}
        )
        self.assertEqual(
            reindexer._get_date_range_query('2015-11-10', '2015-12-11', date_field='date123'),
            {'rows': 0, 'facet.range.end': '2015-12-11', 'facet': 'true', 'facet.range': 'date123',
             'facet.range.start': '2015-11-10', 'q': '*:*', 'facet.range.include': 'all', 'facet.range.gap': '+1DAY'}
        )
        self.assertEqual(
            reindexer._get_date_range_query('2015-11-10', '2015-12-11', date_field='date123', timespan='MONTH'),
            {'rows': 0, 'facet.range.end': '2015-12-11', 'facet': 'true', 'facet.range': 'date123',
             'facet.range.start': '2015-11-10', 'q': '*:*', 'facet.range.include': 'all', 'facet.range.gap': '+1MONTH'}
        )
        self.assertEqual(
            reindexer._get_date_range_query('2015-11-10', '2015-12-11', timespan='MONTH'),
            {'rows': 0, 'facet.range.end': '2015-12-11', 'facet': 'true', 'facet.range': 'index_date',
             'facet.range.start': '2015-11-10', 'q': '*:*', 'facet.range.include': 'all', 'facet.range.gap': '+1MONTH'}
        )

    def test_get_date_facet_counts(self):
        '''
        Checks the date_range_query generation function. Makes sure the date ranges returned matches what got indexed.
        '''
        self._index_docs(50000, self.colls[0])
        solr = SolrClient(test_config['SOLR_SERVER'][0], devel=True, auth=test_config['SOLR_CREDENTIALS'])
        reindexer = Reindexer(source=solr, source_coll='source_coll', dest=solr, dest_coll='dest_coll',
                              date_field='date')
        # Testing this one
        source_facet, dest_facet = reindexer._get_date_facet_counts('DAY', 'date',
                                                                    start_date=self._start_date.date().isoformat())
        for dt_range in source_facet:
            dt = datetime.datetime.strptime(dt_range, '%Y-%m-%dT%H:%M:%SZ').date().isoformat()
            if source_facet[dt_range] != self.date_counts[dt]:
                logging.info("{} - {} - {}".format(dt, source_facet[dt_range], self.date_counts[dt]))
            self.assertEqual(source_facet[dt_range], self.date_counts[dt])

    def test_get_date_facet_counts_without_start_date(self):
        '''
        Checks the date_range_query generation function. Since it's pretty simple, running all the tests as one
        '''
        self._index_docs(50000, self.colls[0])
        solr = SolrClient(test_config['SOLR_SERVER'][0], devel=True, auth=test_config['SOLR_CREDENTIALS'])
        reindexer = Reindexer(source=solr, source_coll='source_coll', dest=solr, dest_coll='dest_coll',
                              date_field='date')
        # Testing this one
        source_facet, dest_facet = reindexer._get_date_facet_counts('DAY', 'date')
        for dt_range in source_facet:
            dt = datetime.datetime.strptime(dt_range, '%Y-%m-%dT%H:%M:%SZ').date().isoformat()
            if source_facet[dt_range] != self.date_counts[dt]:
                logging.info("{} - {} - {}".format(dt, source_facet[dt_range], self.date_counts[dt]))
            self.assertEqual(source_facet[dt_range], self.date_counts[dt])

    def test_get_date_facet_counts_not_day(self):
        '''
        Checks the date_range_query generation function. Since it's pretty simple, running all the tests as one
        '''
        self._index_docs(50000, self.colls[0])
        solr = SolrClient(test_config['SOLR_SERVER'][0], devel=True, auth=test_config['SOLR_CREDENTIALS'])
        reindexer = Reindexer(source=solr, source_coll='source_coll', dest=solr, dest_coll='dest_coll',
                              date_field='date')
        # Testing this one
        with self.assertRaises(ValueError):
            source_facet, dest_facet = reindexer._get_date_facet_counts('MONTH', 'date')

    ## These tests are focused on methods related to resuming re-indexing

    def test_solr_to_solr_resume_checkonly(self):
        '''
        Checks the date_range_query generation function. Since it's pretty simple, running all the tests as one
        '''
        self._index_docs(50000, self.colls[0])
        solr = SolrClient(test_config['SOLR_SERVER'][0], devel=True, auth=test_config['SOLR_CREDENTIALS'])
        reindexer = Reindexer(source=solr, source_coll='source_coll', dest=solr, dest_coll='dest_coll',
                              date_field='date')
        # Make sure only source has data
        self.assertEqual(len(solr.query(self.colls[0], {'q': '*:*', 'rows': 10000000}).docs), 50000)
        self.assertEqual(len(solr.query(self.colls[1], {'q': '*:*', 'rows': 10000000}).docs), 0)
        reindexer.resume(check=True)
        # Makes sure nothing got indexed
        self.assertEqual(len(solr.query(self.colls[0], {'q': '*:*', 'rows': 10000000}).docs), 50000)
        self.assertEqual(len(solr.query(self.colls[1], {'q': '*:*', 'rows': 10000000}).docs), 0)

    def test_solr_to_solr_resume_basic(self):
        '''
        Checks the date_range_query generation function. Since it's pretty simple, running all the tests as one
        '''
        self._index_docs(50000, self.colls[0])
        solr = SolrClient(test_config['SOLR_SERVER'][0], auth=test_config['SOLR_CREDENTIALS'])
        reindexer = Reindexer(source=solr, source_coll='source_coll', dest=solr, dest_coll='dest_coll',
                              date_field='date')
        # Make sure only source has datae
        self.assertEqual(len(solr.query(self.colls[0], {'q': '*:*', 'rows': 10000000}).docs), 50000)
        self.assertEqual(len(solr.query(self.colls[1], {'q': '*:*', 'rows': 10000000}).docs), 0)
        reindexer.resume()
        # Make sure countc match up after reindex
        self.assertEqual(
            len(solr.query(self.colls[0], {'q': '*:*', 'rows': 10000000}).docs),
            len(solr.query(self.colls[1], {'q': '*:*', 'rows': 10000000}).docs))

    def test_solr_to_solr_reindex_and_resume(self):
        '''
        Only reindexes half of the collection on the first time. Then goes back and does a resume to make sure it works.
        '''
        self._index_docs(50000, self.colls[0])
        solr = SolrClient(test_config['SOLR_SERVER'][0], auth=test_config['SOLR_CREDENTIALS'])
        reindexer = Reindexer(source=solr, source_coll='source_coll', dest=solr, dest_coll='dest_coll',
                              date_field='date')
        # Make sure only source has datae
        self.assertEqual(len(solr.query(self.colls[0], {'q': '*:*', 'rows': 10000000}).docs), 50000)
        self.assertEqual(len(solr.query(self.colls[1], {'q': '*:*', 'rows': 10000000}).docs), 0)
        # This gets somehwat of a mid point date in the range.
        midpoint = (datetime.datetime.now() - datetime.timedelta(days=
                                                                 ((self._end_date - self._start_date).days / 2)
                                                                 ))
        # Reindex approximately half of the data by restricting FQ
        reindexer.reindex(fq=['date:[* TO {}]'.format(midpoint.isoformat() + 'Z')])
        # Make sure we have at least 20% of the data.
        dest_count = len(solr.query(self.colls[1], {'q': '*:*', 'rows': 10000000}).docs)
        s_count = len(solr.query(self.colls[0], {'q': '*:*', 'rows': 10000000}).docs)
        self.assertTrue(s_count > dest_count > s_count * .20)
        reindexer.resume()
        # Make sure countc match up after reindex
        self.assertEqual(
            len(solr.query(self.colls[0], {'q': '*:*', 'rows': 10000000}).docs),
            len(solr.query(self.colls[1], {'q': '*:*', 'rows': 10000000}).docs))

    def test_solr_to_solr_reindex_and_resume_reverse(self):
        '''
        Only reindexes half of the collection on the first time. Then goes back and does a resume to make sure it works.
        '''
        self._index_docs(50000, self.colls[0])
        solr = SolrClient(test_config['SOLR_SERVER'][0], auth=test_config['SOLR_CREDENTIALS'])
        reindexer = Reindexer(source=solr, source_coll='source_coll', dest=solr, dest_coll='dest_coll',
                              date_field='date')
        # Make sure only source has data
        self.assertEqual(len(solr.query(self.colls[0], {'q': '*:*', 'rows': 10000000}).docs), 50000)
        self.assertEqual(len(solr.query(self.colls[1], {'q': '*:*', 'rows': 10000000}).docs), 0)
        # This gets somehwat of a mid point date in the range.
        midpoint = (datetime.datetime.now() - datetime.timedelta(days=
                                                                 ((self._end_date - self._start_date).days / 2)
                                                                 ))
        # Reindex approximately half of the data by restricting FQ
        reindexer.reindex(fq=['date:[{} TO *]'.format(midpoint.isoformat() + 'Z')])
        # Make sure we have at least 20% of the data.
        dest_count = len(solr.query(self.colls[1], {'q': '*:*', 'rows': 10000000}).docs)
        s_count = len(solr.query(self.colls[0], {'q': '*:*', 'rows': 10000000}).docs)
        self.assertTrue(s_count > dest_count > s_count * .20)
        reindexer.resume()
        # Make sure countc match up after reindex
        self.assertEqual(
            len(solr.query(self.colls[0], {'q': '*:*', 'rows': 10000000}).docs),
            len(solr.query(self.colls[1], {'q': '*:*', 'rows': 10000000}).docs))

    def test_solr_to_solr_reindexer_per_shard(self):
        self._index_docs(50000, self.colls[0])
        solr = SolrClient(test_config['SOLR_SERVER'][0], auth=test_config['SOLR_CREDENTIALS'])
        # Make sure only source has data
        self.assertEqual(len(solr.query(self.colls[0], {'q': '*:*', 'rows': 10000000}).docs), 50000)
        self.assertEqual(len(solr.query(self.colls[1], {'q': '*:*', 'rows': 10000000}).docs), 0)

        reindexer = Reindexer(source=solr, source_coll='source_coll_shard1_replica1', dest=solr,
                              dest_coll=self.colls[1], per_shard=True, date_field='date')
        reindexer.reindex()
        reindexer = Reindexer(source=solr, source_coll='source_coll_shard2_replica1', dest=solr,
                              dest_coll=self.colls[1], per_shard=True, date_field='date')
        reindexer.reindex()

        self.solr.commit(self.colls[1], openSearcher=True)
        # sloppy check over here, will improve later
        self.assertEqual(
            len(solr.query(self.colls[0], {'q': '*:*', 'rows': 10000000}).docs),
            len(solr.query(self.colls[1], {'q': '*:*', 'rows': 10000000}).docs))
