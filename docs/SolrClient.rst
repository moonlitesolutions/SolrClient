SolrClient package
==================
This is the primary module for interacting with Solr. All other components are accessed through a SolrClient instance. 
Basic usage: ::

	>>> from SolrClient import SolrClient
	>>> solr = SolrClient('http://localhost:8983/solr') #Solr URL
	>>> res = solr.query('SolrClient_unittest',{ #Query params are sent as a python dict
                'q':'product_name:Lorem',
                'facet':True,
                'facet.field':'facet_test',
        })
	>>> res.get_results_count() #Number of items returned
	4
	>>> res.get_facets() #If you asked for facets it will give you facets as a python dict
	{'facet_test': {'ipsum': 0, 'sit': 0, 'dolor': 2, 'amet,': 1, 'Lorem': 1}}
	>>> res.docs #Returns documents as a list
	[{'product_name_exact': 'orci. Morbi ipsum ullamcorper, quam', '_version_': 15149272615480197 12, 'facet_test': ['dolor'], 'date': '2015-10-13T14:40:20.492Z', 'id': 'cb666bd1-ab8e-4951-98 29-5ccd4c12d10b', 'price': 10, 'product_name': 'ullamcorper, nulla. Vestibulum Lorem orci,'},  {'product_name_exact': 'enim aliquet orci. sapien, mattis,', '_version_': 151492726156689408 0, 'facet_test': ['dolor'], 'date': '2015-10-13T14:40:20.492Z', 'id': '8cb40255-ea07-4ab2-a30 f-6e843781a043', 'price': 22, 'product_name': 'dui. Lorem ullamcorper, lacus. hendrerit'}, {' product_name_exact': 'arcu In Nunc vel Nunc', '_version_': 1514927261568991234, 'facet_test':  ['Lorem'], 'date': '2015-10-13T14:40:20.493Z', 'id': '287702d2-90b8-4dce-8e66-00a016e51bdd',  'price': 93, 'product_name': 'ipsum vel. Lorem dui. risus'}, {'product_name_exact': 'Vivamus  sem ac dolor neque', '_version_': 1514927261656023040, 'facet_test': ['amet,'], 'date': '201 5-10-13T14:40:20.494Z', 'id': 'f3c396f0-1fc2-4847-a966-1ebe055b8bd7', 'price': 60, 'product_n ame': 'consectetur Mauris dolor Lorem adipiscing'}]



SolrClient.SolrClient module
----------------------------

.. automodule:: SolrClient
.. autoclass:: SolrClient
    :members:
    :undoc-members:
    :show-inheritance:

