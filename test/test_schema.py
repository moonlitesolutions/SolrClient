import unittest
import logging
from SolrClient import SolrClient
from .test_config import test_config

# logging.basicConfig(level=logging.DEBUG,format='%(asctime)s [%(levelname)s] (%(process)d) (%(threadName)-10s) [%(name)s] %(message)s')
logging.disable(logging.CRITICAL)


class schemaTest(unittest.TestCase):
    # High Level Client Tests
    def setUp(self):
        self.solr = SolrClient(test_config['SOLR_SERVER'][0], devel=True, auth=test_config['SOLR_CREDENTIALS'])

    def delete_fields(self):
        # Fix this later to check for field before sending a delete
        for copy_field in self.solr.schema.get_schema_copyfields(test_config['SOLR_COLLECTION']):
            try:
                self.solr.schema.delete_copy_field(test_config['SOLR_COLLECTION'], copy_field)
            except:
                logging.info("tried to delete copy field that didn't exist")
                pass
        for field in test_config['collections']['fields']:
            if self.solr.schema.does_field_exist(test_config['SOLR_COLLECTION'], field['name']):
                self.solr.schema.delete_field(test_config['SOLR_COLLECTION'], field['name'])

    def create_fields(self):
        for field in test_config['collections']['fields']:
            self.solr.schema.create_field(test_config['SOLR_COLLECTION'], field)

    def create_copy_fields(self):
        for field in test_config['collections']['copy_fields']:
            self.solr.schema.create_copy_field(test_config['SOLR_COLLECTION'], field)

    def check_fields(self, result):
        if result is True:
            for field in test_config['collections']['fields']:
                self.assertTrue(self.solr.schema.does_field_exist(test_config['SOLR_COLLECTION'], field['name']))
        elif result is False:
            for field in test_config['collections']['fields']:
                self.assertFalse(self.solr.schema.does_field_exist(test_config['SOLR_COLLECTION'], field['name']))

    def test_create_fields(self):
        # Delete Fields if they exist
        self.delete_fields()
        # Check to Make Sure fields were deleted
        self.check_fields(False)
        # Create new Fields
        self.create_fields()
        # Make sure they are there
        self.check_fields(True)
        # Create Copy Fields
        self.create_copy_fields()

    def test_check_if_field_exists(self):
        self.assertTrue(self.solr.schema.does_field_exist(test_config['SOLR_COLLECTION'], 'id'))
        self.assertFalse(self.solr.schema.does_field_exist(test_config['SOLR_COLLECTION'], 'asdf432qftr43fsfgreg'))


if __name__ == '__main__':
    pass
