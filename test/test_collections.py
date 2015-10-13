import unittest
from solrclient import SolrClient
from .test_config import test_config
import logging
from time import sleep
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s [%(levelname)s] (%(process)d) (%(threadName)-10s) [%(name)s] %(message)s')

class CollectionsTest(unittest.TestCase):
    #High Level Client Tests
    def setUp(self):
        self.solr = SolrClient(test_config['SOLR_SERVER'][0],devel=True)

    def test_check_if_field_exists(self):
        self.assertTrue(self.solr.collections.does_field_exist(test_config['SOLR_COLLECTION'],'id'))
        self.assertFalse(self.solr.collections.does_field_exist(test_config['SOLR_COLLECTION'],'asdf432qftr43fsfgreg'))

    def test_create_fields(self):
        pause = 6
        #Delete Fields if they exist
        self.delete_fields()
        sleep(pause)
        #Check to Make Sure fields were deleted
        sleep(pause)
        self.check_fields(False)
        #Create new Fields
        sleep(pause)
        self.create_fields()
        #Make sure they are there
        sleep(pause)
        self.check_fields(True)
        #Create Copy Fields
        self.create_copy_fields()
        sleep(pause)

    def delete_fields(self):
        for field in test_config['collections']['fields']:
            if self.solr.collections.does_field_exist(test_config['SOLR_COLLECTION'],field['name']):
                self.solr.collections.delete_field(test_config['SOLR_COLLECTION'],field['name'])
        
    def create_fields(self):
        for field in test_config['collections']['fields']:
            self.solr.collections.create_field(test_config['SOLR_COLLECTION'],field)
    
    def create_copy_fields(self):
        for field in test_config['collections']['copy_fields']:
            self.solr.collections.create_copy_field(test_config['SOLR_COLLECTION'],field)
    
    def check_fields(self,result):
        if result is True:
            for field in test_config['collections']['fields']:
                self.assertTrue(self.solr.collections.does_field_exist(test_config['SOLR_COLLECTION'],field['name']))
        elif result is False:
            for field in test_config['collections']['fields']:
                self.assertFalse(self.solr.collections.does_field_exist(test_config['SOLR_COLLECTION'],field['name']))
    
if __name__=='__main__':
    pass