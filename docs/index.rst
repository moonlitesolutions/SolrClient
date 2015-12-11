SolrClient |version|
----------
SolrClient is a basic python library for Solr with several helper modules. Built in python3 with support for latest features of Solr 5. Development is heavily focused on indexing as well as parsing various query responses and returning them in native python data structures. Several helper classes will be built to automate querying and management of Solr clusters. 

Requirements
~~~~~~~~~~~~
* python 3
* requests library (http://docs.python-requests.org/en/latest/)
* Solr


Features
~~~~~~~~
* Response Object to easily extract data from Solr Response
* Cursor Mark support
* Indexing: (raw JSON, JSON Files, gzipped JSON)
* Specify multiple hosts/IPs for SolrCloud for redundancy
* Basic Managed Schema field management
* Collection Reindexing/FS Export with cursorMark (Solr 4.9+)
* Tested against a real Solr instance


Getting Started
~~~~~~~~~~~~~~~
Basic usage: ::

	>>> from SolrClient import SolrClient
	>>> solr = SolrClient('http://localhost:8983/solr')
	>>> res = solr.query('SolrClient_unittest',{
                'q':'product_name:Lorem',
                'facet':True,
                'facet.field':'facet_test',
        })
	>>> res.get_results_count()
	4
	>>> res.get_facets()
	{'facet_test': {'ipsum': 0, 'sit': 0, 'dolor': 2, 'amet,': 1, 'Lorem': 1}}
	>>> res.get_facet_keys_as_list('facet_test')
	['ipsum', 'sit', 'dolor', 'amet,', 'Lorem']
	>>> res.docs
	[{'product_name_exact': 'orci. Morbi ipsum ullamcorper, quam', '_version_': 15149272615480197 12, 'facet_test': ['dolor'], 'date': '2015-10-13T14:40:20.492Z', 'id': 'cb666bd1-ab8e-4951-98 29-5ccd4c12d10b', 'price': 10, 'product_name': 'ullamcorper, nulla. Vestibulum Lorem orci,'},  {'product_name_exact': 'enim aliquet orci. sapien, mattis,', '_version_': 151492726156689408 0, 'facet_test': ['dolor'], 'date': '2015-10-13T14:40:20.492Z', 'id': '8cb40255-ea07-4ab2-a30 f-6e843781a043', 'price': 22, 'product_name': 'dui. Lorem ullamcorper, lacus. hendrerit'}, {' product_name_exact': 'arcu In Nunc vel Nunc', '_version_': 1514927261568991234, 'facet_test':  ['Lorem'], 'date': '2015-10-13T14:40:20.493Z', 'id': '287702d2-90b8-4dce-8e66-00a016e51bdd',  'price': 93, 'product_name': 'ipsum vel. Lorem dui. risus'}, {'product_name_exact': 'Vivamus  sem ac dolor neque', '_version_': 1514927261656023040, 'facet_test': ['amet,'], 'date': '201 5-10-13T14:40:20.494Z', 'id': 'f3c396f0-1fc2-4847-a966-1ebe055b8bd7', 'price': 60, 'product_n ame': 'consectetur Mauris dolor Lorem adipiscing'}]

See, so easy a caveman can do it. 

Components
----------
.. toctree::
   :maxdepth: 1

   Solr Client <SolrClient>
   Solr Response <SolrResponse>
   Index Queue <IndexQ>
   Schema <Schema>
   Reindexer <Reindexer>

Roadmap
-------
* Better test coverage
* HTTPS Support
* urllib support
* Collection Alias Management
* IndexManager for storing indexing documents off-line and batch indexing them
* More Schema API Action Calls
* Collections API Support

Contributing
------------
I've realized that that there isn't really a well maintained Solr Python library I liked so I put this together. 
Contributions (code, tests, documentation) are definitely welcome; if you have a question about development please open up an issue on github page. 
If you have a pull request, please make sure to add tests and that all of them pass before submitting. 
