SolrClient.IndexQ module
-----------------------------
Really simple filesystem based queue for de-coupling data getting/processing from indexing into solr. All in all, this module is nothing special, but I suspect a lot of people write something similar; so maybe it will save you some time. 

For example, lets say you are working with some data processing that exhibits the following:

- Outputs a large number of items
- These items are possibly small
- You want to process them as fast as possible
- They don't have to be indexed right away

Log parsing is a good example; if you wanted to parse a log file and index that data into Solr you would not send an individual update request for each line and instead aggregate them into something more substantial. This can especially become a problem if you are parsing log files with some parallelism. 

This is the issue that this sub module resolves. It allows you to create a quick file system based queue of items and then index them into Solr later. It will also maintain an internal buffer and add items to it until a specific size is reached before writing it out to the file system. 

Here is the really basic example to illustrate the concept.::
	
	>>> from SolrClient import SolrClient, IndexQ
	>>> index = IndexQ('.','testq',size=1)
	>>> index.add({'id':'test1'})
	17 #By default it returns the buffer offset
	>>> index.get_all_as_list()
	[]
	>>> index.add(finalize=True)
	'./testq/todo/testq_2015-10-20-19-7-58-5219.json' #If file was written it will return the filename
	>>> index.get_all_as_list()
	['./testq/todo/testq_2015-10-20-19-7-58-5219.json']
	>>> solr = SolrClient('http://localhost:7050/solr')
	>>> index.index(solr,'SolrClient_unittest')


Note that you don't have to track the output of add method, it is just there to give you a better idea of what it is doing. You can also specify threads to index method to run this quicker, by default it will use one thread. There is also some logging to provide you a better idea of what it is doing. 


.. automodule:: SolrClient
.. autoclass:: IndexQ
    :members:
    :undoc-members:
    :show-inheritance: