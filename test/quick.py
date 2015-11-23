#Used for some quick troubleshooting
import code
import time
import os
import sys
print(sys.path)
from SolrClient import SolrClient, Reindexer, IndexQ
import logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s [%(levelname)s] (%(process)d) (%(threadName)-10s) [%(name)s] %(message)s')

index = IndexQ('/tmp','test_indexq')
solr = SolrClient('http://localhost:8983/solr/')
r = Reindexer(source=solr,source_coll='source_coll',dest=index)
code.interact(local=locals())

import code
code.interact(local=locals())
