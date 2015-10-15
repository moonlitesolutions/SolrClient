SolrClient.Schema module
-----------------------------
Solr Schema component for basic interations::

	>>> field = {'name':'fieldname','stored':True,'indexed':True,'type':'tdate'}
	>>> solr.schema.create_field('SolrClient_unittest', field)
	{'responseHeader': {'status': 0, 'QTime': 85}}
	>>> solr.schema.does_field_exist('SolrClient_unittest','fieldname')
	True
	>>> res = solr.schema.create_field('SolrClient_unittest', field)
	Traceback (most recent call last):
	  File "<stdin>", line 1, in <module>
	  File "C:\Users\Nick\Documents\GitHub\SolrClient\SolrClient\schema.py", line 43, in create_field
		raise ValueError("Field {} Already Exists in Solr Collection {}".format(field_dict['name'],collection))
	ValueError: Field fieldname Already Exists in Solr Collection SolrClient_unittest


.. automodule:: SolrClient
.. autoclass:: Schema
    :members:
    :undoc-members:
    :show-inheritance: