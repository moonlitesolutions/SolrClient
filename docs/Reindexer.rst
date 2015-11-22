SolrClient.Reindexer module
-----------------------------
This helper class uses Solr's cursorMark (Solr 4.9+)  to re-index collections or to dump out your collection to filesystem. It is useful if you want to get an offline
snapshot of your data. Additionally, you will need to re-index your data to upgrade lucene indexes and this is a handy way to do it. 

In the most basic way; it will sort your items by id and page through the results in batches of `rows` and concurrently send data to the destiation. 
Destination can be either another Solr collection or an IndexQ instance. If it is another Solr collection you have to make sure that it is configured exactly
as the first one. Keep in mind that if items are added or modified while you are performing this operation; they may not be captured. So it is advised
to stop indexing while you are running it. 

If you are keeping the document's index timestamp, with something like::

<field name="last_update" type="date" indexed="true" stored="true" default="NOW" />

You can specify that field through `date_field` parameter. If it is supplied, the Reindexer will include the date_field in the sort and 
start re-indexing starting with the oldest documents. This way new items will also be picked up. Note that deletions will not be carried over so it is still adviced to stop indexing. 

Using this will also allows you to resume the reindexing if it gets interrupted for some reason through the `resume` method. 

On the resume, it will run several range facet queries to compare the counts based on date ranges and only re-process the ranges that have missing documents. 

.. automodule:: SolrClient.helpers
.. autoclass:: Reindexer
    :members:
    :undoc-members:
    :show-inheritance:
