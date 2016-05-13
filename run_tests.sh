#!/bin/bash
export SOLR_COLLECTION=SolrClient_unittest

for version in "5.2.1" "5.3.1" "6.0.0"
do
  export SOLR_TEST_URL=http://localhost:9`echo $version | sed -r 's/\.//g'`/solr
  python -m unittest test.test_client
  python -m unittest test.test_resp
  python -m unittest test.test_indexq
  python -m unittest test.test_collections
  python -m unittest test.test_reindexer
done
