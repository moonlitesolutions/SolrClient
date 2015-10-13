import logging
import json

class Schema():
    '''
        Manages Solr Collections
    '''
    def __init__(self,solr):
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
        : collection = name of the colleciton to pull schema from
        '''
        return self.solr.transport.send_request(endpoint='schema/fields',collection=collection)
        
    
    def get_schema_copyfields(self, collection):
        return self.solr.transport.send_request(endpoint='schema/copyfields',collection=collection)
    

    def create_field(self, collection, field_dict):
        '''
        Creates a new field in managed schema. field_dict should look like this::
            {
             "name":"sell-by",
             "type":"tdate",
             "stored":true }
        '''
        if self.does_field_exist(collection,field_dict['name']):
            raise ValueError("Field {} Already Exists in Solr Collection {}".format(field_dict['name'],collection))
        temp = {"add-field":dict(field_dict)}
        return self.solr.transport.send_request(method='POST',endpoint=self.schema_endpoint,collection=collection, data=json.dumps(temp))

        
    def delete_field(self,collection,field_name):
        """
        Deletes a field from the Solr Collection
        """
        if not self.does_field_exist(collection,field_name):
            raise ValueError("Field {} Doesn't Exists in Solr Collection {}".format(field_name,collection))
        else:
            temp = {"delete-field" : { "name":field_name }}
            return self.solr.transport.send_request(method='POST',endpoint=self.schema_endpoint,collection=collection, data=json.dumps(temp))

            
    def does_field_exist(self,collection,field_name):
        schema = self.get_schema_fields(collection)
        return True if field_name in [field['name'] for field in schema['fields']] else False
    
    def create_copy_field(self,collection,copy_dict):
        temp = {"add-copy-field":dict(copy_dict)}
        return self.solr.transport.send_request(method='POST',endpoint=self.schema_endpoint,collection=collection, data=json.dumps(temp))
        
 
    def delete_copy_field(self,collection,copy_dict):
        temp = {"delete_copy_field":dict(copy_dict)}
        return self.solr.transport.send_request(method='POST',endpoint=self.schema_endpoint,collection=collection, data=json.dumps(temp))
    
    def get_schema_copyfields(self,collection):
        return self.solr.transport.send_request(endpoint='schema/copyfields',collection=collection)['copyFields']
        