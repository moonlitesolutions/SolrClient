import gzip
import os
import json
import logging
from .transport import TransportRequests
from .schema import Schema
from .exceptions import *
from .solrresp import SolrResponse

class SolrClient:
    '''
    
    Creates a new SolrClient.
    
    :param host: Specifies the location of Solr Server. ex 'http://localhost:8983/solr'. Can also take a list of host values in which case it will use the first server specified, but will switch over to the second one if the first one is not available. 
    :param transport: Transport class to use. So far only requests is supported. 
    :param bool devel: Can be turned on during development or debugging for a much greater logging. Requires logging to be configured with DEBUG level. 
    '''
    def __init__(self, host='http://localhost:8983/solr', transport=TransportRequests,devel=False,auth=None):
  
        self.devel = devel
        self.host = host
        
        self.logger = logging.getLogger(__package__)
        self.schema = Schema(self)
        self.transport = transport(self,host=host,auth=auth)
        
    def commit(self,collection,openSearcher=False,softCommit=False,waitSearcher=True,commit=True,**kwargs):
        '''
        :param str collection: The name of the collection for the request
        :param bool openSearcher: If new searcher is to be opened
        :param bool softCommit: SoftCommit
        :param bool waitServer: Blocks until the new searcher is opened
        :param book commit: Commit
        
        Sends a commit to a Solr collection. 
        
        '''
        comm = {
            'openSearcher':str(openSearcher).lower(),
            'softCommit':str(softCommit).lower(),
            'waitSearcher':str(waitSearcher).lower(),
            'commit':str(commit).lower()
        }
            
        self.logger.debug("Sending Commit to Collection {}".format(collection))
        try:
            resp = self.transport.send_request(method='GET',endpoint='update',collection=collection, params=comm,**kwargs)
        except Exception as e:
            raise
        self.logger.debug("Commit Successful, QTime is {}".format(resp['responseHeader']['QTime']))
        
        
    def query(self,collection,query,request_handler='select',**kwargs):
        """
        :param str collection: The name of the collection for the request
        :param str request_handler: Request handler, default is 'select'
        :param dict query: Python dictonary of Solr query parameters. 
        
        Sends a query to Solr, returns a SolrResults Object. `query` should be a dictionary of solr request handler arguments.
        Example::
        
            res = solr.query('SolrClient_unittest',{
                'q':'*:*',
                'facet':True,
                'facet.field':'facet_test',
            })
   
        """
        for field in ['facet.pivot']:
            if field in query.keys():
                if type(query[field]) is str:
                    query[field] = query[field].replace(' ','')
                elif type(query[field]) is list:
                    query[field] = [s.replace(' ','') for s in query[field]]
         
        return(SolrResponse(self.transport.send_request(method='GET',endpoint=request_handler,collection=collection, params=query,*kwargs)))
            
        
    def index_json(self,collection,data,params={},**kwargs):
        '''
        :param str collection: The name of the collection for the request.
        :param data str data: Valid Solr JSON as a string. ex: '[{"title": "testing solr indexing", "id": "test1"}]'
        
        Sends supplied json to solr for indexing, supplied JSON must be a list of dictionaries.  ::
        
            >>> docs = [{'id':'changeme','field1':'value1'},
                        {'id':'changeme1','field2':'value2'}]
            >>> solr.index_json('SolrClient_unittest',json.dumps(docs))
            
        '''
        return self.transport.send_request(method='POST',endpoint='update',collection=collection, data=data,params=params,*kwargs)
        
    def delete_doc_by_id(self,collection,id,**kwargs):
        '''
        :param str collection: The name of the collection for the request
        :param str id: ID of the document to be deleted. Can specify '*' to delete everything. 
        
        Deletes items from Solr based on the ID. ::
            
            >>> solr.delete_doc_by_id('SolrClient_unittest','changeme')
        
        '''
        temp = {"delete": {"query":"id:{}".format(id)}}
        return self.transport.send_request(method='POST', endpoint='update', collection=collection, data=json.dumps(temp), *kwargs)
        
    def stream_file(self,collection,filename,**kwargs):
        '''
        
        :param str collection: The name of the collection for the request
        :param str filename: Filename of json file to index. 
        
        Will open the json file, uncompressing it if necessary, and submit it to specified solr collection for indexing. 
        ::
        
            >>> solr.local_index('SolrClient_unittest',
                                       '/local/to/script/temp_file.json')
        
        '''
        import os
        if os.path.isfile(filename):
            self.logger.info("Indexing {} into Solr Collection {}".format(filename,collection))
            if filename.endswith('gz'):
                file =  gzip.open(filename,'r')
            else:
                file = open(filename,'r')
            js_data = file.read()
            file.close()
            return self.index_json(collection,js_data)
        else:
            raise IOError("{} File Not Found".format(file))
   
    def local_index(self,collection,filename,**kwargs):
        '''
        :param str collection: The name of the collection for the request
        :param str filename: String file path of the file to index. 
        
        Will index specified file into Solr. The `file` must be local to the server, this is faster than other indexing options. 
        If the files are already on the servers I suggest you use this. 
        For example::
        
            >>> solr.local_index('SolrClient_unittest',
                                       '/local/to/server/temp_file.json')
        '''
        filename = os.path.abspath(filename)
        self.logger.info("Indexing {} into Solr Collection {}".format(filename,collection))
            
        data = {'stream.file' : filename,
                'stream.contentType' : 'text/json'}
        res = self.transport.send_request(method='GET', endpoint='update/json', collection=collection, params=data, *kwargs)
        return res
