import gzip
import os
import json
import logging
from .transport import TransportRequests
from .exceptions import NotFoundError, MinRfError
from .schema import Schema
from .solrresp import SolrResponse
from .collections import Collections
from .zk import ZK


class SolrClient(object):
    """
    Creates a new SolrClient.

    :param host: Specifies the location of Solr Server. ex 'http://localhost:8983/solr'. Can also take a list of host values in which case it will use the first server specified, but will switch over to the second one if the first one is not available.
    :param transport: Transport class to use. So far only requests is supported.
    :param bool devel: Can be turned on during development or debugging for a much greater logging. Requires logging to be configured with DEBUG level.
    """

    def __init__(self,
                 host='http://localhost:8983/solr',
                 transport=TransportRequests,
                 devel=False,
                 auth=None,
                 log=None,
                 **kwargs):
        self.devel = devel
        self.host = host
        self.transport = transport(self, auth=auth, devel=devel, host=host, **kwargs)
        self.logger = log if log else logging.getLogger(__package__)
        self.schema = Schema(self)
        self.collections = Collections(self, self.logger)

    def get_zk(self):
        return ZK(self, self.logger)

    def commit(self, collection, openSearcher=False, softCommit=False,
               waitSearcher=True, commit=True, **kwargs):
        """
        :param str collection: The name of the collection for the request
        :param bool openSearcher: If new searcher is to be opened
        :param bool softCommit: SoftCommit
        :param bool waitServer: Blocks until the new searcher is opened
        :param bool commit: Commit

        Sends a commit to a Solr collection.

        """
        comm = {
            'openSearcher': str(openSearcher).lower(),
            'softCommit': str(softCommit).lower(),
            'waitSearcher': str(waitSearcher).lower(),
            'commit': str(commit).lower()
        }

        self.logger.debug("Sending Commit to Collection {}".format(collection))
        try:
            resp, con_inf = self.transport.send_request(method='GET', endpoint='update', collection=collection,
                                                        params=comm, **kwargs)
        except Exception as e:
            raise
        self.logger.debug("Commit Successful, QTime is {}".format(resp['responseHeader']['QTime']))

    def query_raw(self, collection, query, request_handler='select', **kwargs):
        """
        :param str collection: The name of the collection for the request
        :param str request_handler: Request handler, default is 'select'
        :param dict query: Python dictionary of Solr query parameters.

        Sends a query to Solr, returns a dict. `query` should be a dictionary of solr request handler arguments.
        Example::

            res = solr.query_raw('SolrClient_unittest',{
                'q':'*:*',
                'facet':True,
                'facet.field':'facet_test',
            })

        """
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        data = query
        resp, con_inf = self.transport.send_request(method='POST',
                                                    endpoint=request_handler,
                                                    collection=collection,
                                                    data=data,
                                                    headers=headers,
                                                    **kwargs)
        return resp

    def query(self, collection, query, request_handler='select', **kwargs):
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
                    query[field] = query[field].replace(' ', '')
                elif type(query[field]) is list:
                    query[field] = [s.replace(' ', '') for s in query[field]]

        method = 'POST'
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        params = query
        data = {}
        resp, con_inf = self.transport.send_request(method=method,
                                                    endpoint=request_handler,
                                                    collection=collection,
                                                    params=params,
                                                    data=data,
                                                    headers=headers,
                                                    **kwargs)
        if resp:
            resp = SolrResponse(resp)
            resp.url = con_inf['url']
            return resp

    def index(self, collection, docs, params=None, min_rf=None, **kwargs):
        """
        :param str collection: The name of the collection for the request.
        :param docs list docs: List of dicts. ex: [{"title": "testing solr indexing", "id": "test1"}]
        :param min_rf int min_rf: Required number of replicas to write to'

        Sends supplied list of dicts to solr for indexing.  ::

            >>> docs = [{'id':'changeme','field1':'value1'}, {'id':'changeme1','field2':'value2'}]
            >>> solr.index('SolrClient_unittest', docs)

        """
        data = json.dumps(docs)
        return self.index_json(collection, data, params, min_rf=min_rf, **kwargs)

    def index_json(self, collection, data, params=None, min_rf=None, **kwargs):
        """
        :param str collection: The name of the collection for the request.
        :param data str data: Valid Solr JSON as a string. ex: '[{"title": "testing solr indexing", "id": "test1"}]'
        :param min_rf int min_rf: Required number of replicas to write to'

        Sends supplied json to solr for indexing, supplied JSON must be a list of dictionaries.  ::

            >>> docs = [{'id':'changeme','field1':'value1'},
                        {'id':'changeme1','field2':'value2'}]
            >>> solr.index_json('SolrClient_unittest',json.dumps(docs))

        """
        if params is None:
            params = {}

        resp, con_inf = self.transport.send_request(method='POST',
                                                    endpoint='update',
                                                    collection=collection,
                                                    data=data,
                                                    params=params,
                                                    min_rf=min_rf,
                                                    **kwargs)
        if min_rf is not None:
            rf = resp['responseHeader']['rf']
            if rf < min_rf:
                raise MinRfError("couldn't satisfy rf:%s min_rf:%s" % (rf, min_rf), rf=rf, min_rf=min_rf)
        if resp['responseHeader']['status'] == 0:
            return True
        return False

    def get(self, collection, doc_id, **kwargs):
        """
        :param str collection: The name of the collection for the request
        :param str doc_id: ID of the document to be retrieved.

        Retrieve document from Solr based on the ID. ::

            >>> solr.get('SolrClient_unittest','changeme')
        """

        resp, con_inf = self.transport.send_request(method='GET',
                                                    endpoint='get',
                                                    collection=collection,
                                                    params={'id': doc_id},
                                                    **kwargs)
        if 'doc' in resp and resp['doc']:
            return resp['doc']
        raise NotFoundError

    def mget(self, collection, doc_ids, **kwargs):
        """
        :param str collection: The name of the collection for the request
        :param tuple doc_ids: ID of the document to be retrieved.

        Retrieve documents from Solr based on the ID. ::

            >>> solr.get('SolrClient_unittest','changeme')
        """

        resp, con_inf = self.transport.send_request(method='GET',
                                                    endpoint='get',
                                                    collection=collection,
                                                    params={'ids': doc_ids},
                                                    **kwargs)
        if 'docs' in resp['response']:
            return resp['response']['docs']
        raise NotFoundError

    def delete_doc_by_id(self, collection, doc_id, **kwargs):
        """
        :param str collection: The name of the collection for the request
        :param str id: ID of the document to be deleted. Can specify '*' to delete everything.

        Deletes items from Solr based on the ID. ::

            >>> solr.delete_doc_by_id('SolrClient_unittest','changeme')

        """
        if ' ' in doc_id:
            doc_id = '"{}"'.format(doc_id)
        temp = {"delete": {"query": 'id:{}'.format(doc_id)}}
        resp, con_inf = self.transport.send_request(method='POST',
                                                    endpoint='update',
                                                    collection=collection,
                                                    data=json.dumps(temp),
                                                    **kwargs)
        return resp

    def delete_doc_by_query(self, collection, query, **kwargs):
        """
        :param str collection: The name of the collection for the request
        :param str query: Query selecting documents to be deleted.

        Deletes items from Solr based on a given query. ::

            >>> solr.delete_doc_by_query('SolrClient_unittest','*:*')

        """
        temp = {"delete": {"query": query}}
        resp, con_inf = self.transport.send_request(method='POST',
                                                    endpoint='update',
                                                    collection=collection,
                                                    data=json.dumps(temp),
                                                    **kwargs)
        return resp

    def stream_file(self, collection, filename, **kwargs):
        """

        :param str collection: The name of the collection for the request
        :param str filename: Filename of json file to index.

        Will open the json file, uncompressing it if necessary, and submit it to specified solr collection for indexing.
        ::

            >>> solr.local_index('SolrClient_unittest',
                                       '/local/to/script/temp_file.json')
        """
        if not os.path.isfile(filename):
            raise IOError("{} File Not Found".format(filename))
        self.logger.info("Indexing {} into Solr Collection {}".format(filename, collection))
        if filename.endswith('gz'):
            open_function = gzip.open
        else:
            open_function = open
        with open_function(filename, 'r') as file:
            js_data = file.read()
        return self.index_json(collection, js_data)

    def local_index(self, collection, filename, **kwargs):
        """
        :param str collection: The name of the collection for the request
        :param str filename: String file path of the file to index.

        Will index specified file into Solr. The `file` must be local to the server, this is faster than other indexing options.
        If the files are already on the servers I suggest you use this.
        For example::

            >>> solr.local_index('SolrClient_unittest',
                                       '/local/to/server/temp_file.json')
        """
        filename = os.path.abspath(filename)
        self.logger.info("Indexing {} into Solr Collection {}".format(filename, collection))

        data = {'stream.file': filename,
                'stream.contentType': 'text/json'}
        resp, con_inf = self.transport.send_request(method='GET', endpoint='update/json', collection=collection,
                                                    params=data, **kwargs)
        if resp['responseHeader']['status'] == 0:
            return True
        else:
            return False

    # Version 0.0.7
    def paging_query(self, collection, query, rows=1000, start=0, max_start=200000):
        """
        :param str collection: The name of the collection for the request.
        :param dict query: Dictionary of solr args.
        :param int rows: Number of rows to return in each batch. Default is 1000.
        :param int start: What position to start with. Default is 0.
        :param int max_start: Once the start will reach this number, the function will stop. Default is 200000.

        Will page through the result set in increments of `row` WITHOUT using cursorMark until it has all items \
        or until `max_start` is reached. Use max_start to protect your Solr instance if you are not sure how many items you \
        will be getting. The default is 200,000, which is still a bit high.

        Returns an iterator of SolrResponse objects. For Example::

            >>> for res in solr.paging_query('SolrClient_unittest',{'q':'*:*'}):
                    print(res)

        """
        query = dict(query)
        while True:
            query['start'] = start
            query['rows'] = rows
            res = self.query(collection, query)
            if res.get_results_count():
                yield res
                start += rows
            if res.get_results_count() < rows or start > max_start:
                break

    def cursor_query(self, collection, query):
        """
        :param str collection: The name of the collection for the request.
        :param dict query: Dictionary of solr args.

        Will page through the result set in increments using cursorMark until it has all items. Sort is required for cursorMark \
        queries, if you don't specify it, the default is 'id desc'.

        Returns an iterator of SolrResponse objects. For Example::

            >>> for res in solr.cursor_query('SolrClient_unittest',{'q':'*:*'}):
                    print(res)
        """
        cursor = '*'
        if 'sort' not in query:
            query['sort'] = 'id desc'
        while True:
            query['cursorMark'] = cursor
            # Get data with starting cursorMark
            results = self.query(collection, query)
            if results.get_results_count():
                cursor = results.get_cursor()
                yield results
            else:
                self.logger.debug("Got zero Results with cursor: {}".format(cursor))
                break
