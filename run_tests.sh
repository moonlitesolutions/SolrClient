#!/bin/bash
export SOLR_COLLECTION=SolrClient_unittest

for version in "5.2.1" "5.3.1" "6.0.0"
do
  export SOLR_TEST_URL=http://localhost:9`echo $version | sed -r 's/\.//g'`/solr
  python3 -m unittest test.test_client
  python3 -m unittest test.test_resp
  python3 -m unittest test.test_indexq
  python3 -m unittest test.test_collections
  python3 -m unittest test.test_reindexer
done
