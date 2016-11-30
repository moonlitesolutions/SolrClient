import logging
import json
from .exceptions import *
class Schema():
    '''
        Class for interacting with Solr collections that are using data driven schemas.
        At this point there are basic methods for creating/deleting fields, contributions to this class are very welcome.

        More info on Solr can be found here: https://cwiki.apache.org/confluence/display/solr/Schema+API

    '''
    def __init__(self, solr):
        self.solr = solr
        self.coll_api = "/admin/collections"
        self.logger = logging.getLogger('SolrClient.Schema')

        self.devel=False
        if self.solr.devel:
            self.devel=True

        self.schema_endpoint = 'schema/'

    def get_schema_fields(self, collection):
        '''
        Returns Schema Fields from a Solr Collection
        '''
        res, con_info = self.solr.transport.send_request(endpoint='schema/fields',collection=collection)
        return res

    def get_schema_copyfields(self, collection):
        res, con_info = self.solr.transport.send_request(endpoint='schema/copyfields', collection=collection)
        return res['copyFields']


    def create_field(self, collection, field_dict):
        '''
        Creates a new field in managed schema, will raise ValueError if the field already exists.  field_dict should look like this::

            {
                 "name":"sell-by",
                 "type":"tdate",
                 "stored":True
            }

        Reference: https://cwiki.apache.org/confluence/display/solr/Defining+Fields

        '''
        if self.does_field_exist(collection,field_dict['name']):
            raise ValueError("Field {} Already Exists in Solr Collection {}".format(field_dict['name'],collection))
        temp = {"add-field":dict(field_dict)}
        res, con_info =self.solr.transport.send_request(method='POST',endpoint=self.schema_endpoint,collection=collection, data=json.dumps(temp))
        return res

    def replace_field(self, collection, field_dict):
        '''
        Replace a field in managed schema, will raise ValueError if the field does not exist. field_dict as in create_field(...).

        :param string collection:
        :param dict field_dict:
        '''

        if not self.does_field_exist(collection, field_dict['name']):
            raise ValueError("Field {} does not exists in Solr Collection {}".format(field_dict['name'], collection))
        temp = {"replace-field": dict(field_dict)}
        res, con_info = self.solr.transport.send_request(method='POST', endpoint=self.schema_endpoint,
                                                         collection=collection, data=json.dumps(temp))
        return res
    

    def delete_field(self,collection,field_name):
        '''
        Deletes a field from the Solr Collection. Will raise ValueError if the field doesn't exist.

        :param string collection: Name of the collection for the action
        :param string field_name: String name of the field.
        '''
        if not self.does_field_exist(collection,field_name):
            raise ValueError("Field {} Doesn't Exists in Solr Collection {}".format(field_name,collection))
        else:
            temp = {"delete-field" : { "name":field_name }}
            res, con_info = self.solr.transport.send_request(method='POST',endpoint=self.schema_endpoint,collection=collection, data=json.dumps(temp))
            return res


    def does_field_exist(self,collection,field_name):
        '''
        Checks if the field exists will return a boolean True (exists) or False(doesn't exist).

        :param string collection: Name of the collection for the action
        :param string field_name: String name of the field.
        '''
        schema = self.get_schema_fields(collection)
        logging.info(schema)
        return True if field_name in [field['name'] for field in schema['fields']] else False

    def create_copy_field(self,collection,copy_dict):
        '''
        Creates a copy field.

        copy_dict should look like ::

            {'source':'source_field_name','dest':'destination_field_name'}

        :param string collection: Name of the collection for the action
        :param dict copy_field: Dictionary of field info

        Reference: https://cwiki.apache.org/confluence/display/solr/Schema+API#SchemaAPI-AddaNewCopyFieldRule
        '''
        temp = {"add-copy-field":dict(copy_dict)}
        res, con_info = self.solr.transport.send_request(method='POST',endpoint=self.schema_endpoint,collection=collection, data=json.dumps(temp))
        return res

    def delete_copy_field(self, collection, copy_dict):
        '''
        Deletes a copy field.

        copy_dict should look like ::

            {'source':'source_field_name','dest':'destination_field_name'}

        :param string collection: Name of the collection for the action
        :param dict copy_field: Dictionary of field info
        '''

        #Fix this later to check for field before sending a delete
        if self.devel:
            self.logger.debug("Deleting {}".format(str(copy_dict)))
        copyfields = self.get_schema_copyfields(collection)
        if copy_dict not in copyfields:
            self.logger.info("Fieldset not in Solr Copy Fields: {}".format(str(copy_dict)))
        temp = {"delete-copy-field": dict(copy_dict)}
        res, con_info = self.solr.transport.send_request(method='POST',endpoint=self.schema_endpoint,collection=collection, data=json.dumps(temp))
        return res
