import datetime
import logging
from SolrClient import SolrClient

import sys
import gzip
from time import time
import argparse
import os
import json

from datetime import datetime


class Reindexer():
    '''
    This helper class is used to re-index collections. 
    
    At times, such as when upgrading Solr and Lucene indexes, it may be necessary to re-index a collection. Solr 4.9 introduced cursorMark that
    helps with deep paging and using this functionality we can effectively go through all the items in the collection to re-index them. 
    
    This class will run the re-indexing for you and support things like resuming and date based sorting. This module can also output the collection information
    as JSON files to your filesystem. 
    '''
    def __init__(self,
                source,
                dest,
                source_coll=None,
                dest_coll=None,
                rows=1000,
                date_field=None,
                devel=False,
                ):
        '''
        Initiates the re-indexer. 
        
        :param source: An instance of SolrClient or a directory with JSON files. 
        :param dest: An instance of SolrClient or a directory for saving JSON files to. 
        :param string source_coll: Source collection name. 
        :param string dest_coll: Destination collection name
        :param int rows: Number of items to get in each query. Default is 1000. 
        :param string date_field: String name of a Solr date field. 
        :param bool devel: Whenever to turn on super verbouse logging for development. 
        '''
        self.log = logging.getLogger('reindexer')
        self.devel = devel
        if type(source) is SolrClient and source_coll:
            self.source=source
            self.source_coll = source_coll
        
        
        ------
        
        
GLOB = {}
GLOB['docs'] = 0

def run():
    if args.json is not None:
        GLOB['jsondir'] = args.json
    if args.dsolr is not None and args.dcoll is not None:
        GLOB['dsolr'] = SolrServer(args.dsolr,args.dcoll,auth=[args.user,args.password])
    if 'ssolr' in GLOB:
        GLOB['get'] = from_solr
    else:
        GLOB['get'] = from_json
    if 'dsolr' in GLOB:
        GLOB['put'] = to_solr
    else:
        GLOB['put'] = to_json
    
    if args.resume:
        oldest_date = GLOB['ssolr'].query_solr({'q':'*:*',
                                                'rows':1,
                                                'fq':'+{}:*'.format(args.timefield),
                                                'sort':'{} asc'.format(args.timefield)}).docs[0][args.timefield]
        newest_date = GLOB['ssolr'].query_solr({'q':'*:*',
                                                'rows':1,
                                                'fq':'+{}:*'.format(args.timefield),
                                                'sort':'{} desc'.format(args.timefield)}).docs[0][args.timefield]
                                                
        logging.info("Processing Documents between {} and {}".format(oldest_date,newest_date))
        return resume(oldest_date,'MONTH')

    else:
        process_items()

def resume(sdate,timespan):
    source_facet = get_date_range_facets(GLOB['ssolr'],sdate,timespan)
    dest_facet = get_date_range_facets(GLOB['dsolr'],sdate,timespan)
    
    for dt_range in sorted(source_facet):
        if dt_range in dest_facet:
            if args.check:
                logging.info("Date Range: {} Source: {} Destination:{} Difference:{}".format(dt_range,source_facet[dt_range],dest_facet[dt_range], (source_facet[dt_range]-dest_facet[dt_range])))
                continue
                
            if source_facet[dt_range] != dest_facet[dt_range]:
                logging.info("Date Range {} Doesn't match up Source: {}, Destination: {}. Starting to Fix".format(dt_range,source_facet[dt_range],dest_facet[dt_range]))
                if 'MONTH' in timespan:
                    resume(dt_range,'DAY')
                elif not args.check:
                    process_items(fq='{}:[{} TO {}]'.format(args.timefield,dt_range,dt_range+'+1{}'.format(timespan)))
                logging.info("Complete Date Range {}".format(dt_range))
        else:
            logging.error("Source: {}".format(source_facet))
            logging.error("Destination: {}".format(dest_facet))
            raise ValueError("Date Ranges don't match up")
    return 
    
def get_date_range_facets(solr,date,timespan):
    query ={'q':'*:*',
            'rows':0,
            'facet':'true',
            'facet.range':args.timefield,
            'facet.range.gap':'+1{}'.format(timespan),
            'facet.range.end':'NOW+1{}'.format(timespan),
            'facet.range.start':'{}/{}'.format(date,timespan.upper())}
    if args.pershard:
        query['distrib'] = 'false'
    return solr.query_solr(query).get_facets_ranges()[args.timefield]

def process_items(**kwargs):
    stime = time()
    for items_from_solr in GLOB['get'](**kwargs):
        if items_from_solr:
            res = GLOB['put'](items_from_solr)
            if not res:
                logging.error("Failed to Process File ....")
                [logging.error("Failed on ID {}".format(d['id'])) for d in items_from_solr if 'id' in d]
            elif GLOB['docs'] % 10000 == 0:
                logging.info("Processed {} Documents in {} Seconds at {} docs per second".format(GLOB['docs'],(time() - stime),(GLOB['docs']/(time() - stime))))
            if GLOB['docs'] % 1000000 == 0:
                GLOB['dsolr'].send_commit()
    logging.debug("No more items, finishing")
    GLOB['dsolr'].send_commit()
    
def get_query(cursor):
    query = {'q':'*:*',
            'sort':'id desc',
            'rows':args.rows,
            'cursorMark':cursor}
            
    if args.last:
        query['fq'] = '{}:[{}Z-{}DAY TO NOW]'.format(args.timefield,datetime.utcnow().isoformat(),args.last)
        
    if args.timefield:
        query['sort'] = "{} asc, id desc".format(args.timefield)
        
    if args.pershard:
        query['distrib'] = 'false'
    return query
    
def from_solr(fq=None):
    cursor = '*'
    cursorworks=''
    while True:
        query = get_query(cursor)
        if fq:
            query['fq'] = fq
        results = GLOB['ssolr'].query_solr(query)
        if results.get_results_count() == 0:
            logging.debug("Got zero Results with cursor: {}".format(cursor))
            logging.error(results.data)
            return False
        else:
            GLOB['docs'] += results.get_results_count()
            cursorworks=cursor
            cursor = results.get_cursor()
            logging.debug("Got Cursor: {}".format(cursor))
            yield results.docs

def from_json():
    list = [x for x in os.listdir(args.json) if x.endswith('.json') or x.endswith('.json.gz')]
    full = [os.path.join(args.json,x) for x in list]
    full.sort(key=lambda x: os.path.getmtime(x))
    for file in full:
        logging.info("Processing {}".format(file))
        with gzip.open(file, "rb") as f:
            d = json.loads(f.read().decode("ascii"))
            GLOB['docs'] += len(d)
        yield d

def to_json(data):
    file = '{}-{}.json'.format(args.scoll[0],datetime.now().isoformat())
    with open(args.json+os.sep+file,'w') as fh:
        fh.write(json.dumps(data,sort_keys=True))
    _compress(args.json+file)
    return True

def to_solr(data):
    for dat in data:
        del(dat['_version_'])
        fields = [x for x in dat if x.endswith('_exact')]
        for field in fields:
            del(dat[field])
    return GLOB['dsolr']._send(json.dumps(data,sort_keys=True))
    
def _compress(file):
    logging.debug("Compressing {}".format(file))
    os.system("gzip {}".format(file))
    
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to Parse JBOSS Access/Server log files and index the data into Solr')
    parser.add_argument('-ssolr', type=str, help='Source Solr Server (ex. http://localhost:7091:solr/)')
    parser.add_argument('-dsolr', type=str, help='Destination Solr Server (ex. http://localhost:7091:solr/)')
    parser.add_argument('-v', action='store_true', help='verbose logging')
    parser.add_argument('-pershard', action='store_true', help='Whenever to add distrib=False to the query')
    parser.add_argument('-resume', action='store_true', help='Whenever to resume a previously stopped process. Requires a date field.')
    parser.add_argument('-check', action='store_true', help='Checks on progress of resume and prints stats.')
    parser.add_argument('-scoll', type=str, help='Source Collection (ex products_2)')
    parser.add_argument('-dcoll', type=str, help='Destination Collection (ex products_1)')
    parser.add_argument('-json', type=str, help='Instead of indexing, write JSON. Specify Destination Here. (ex. /tmp/json/)')
    parser.add_argument('-user', type=str, help='User for Solr Basic Auth. ')
    parser.add_argument('-password', type=str, help='Password for Solr Basic Auth. ')
    parser.add_argument('-timefield', type=str, default='timestamp', help='Time Field (ex. timestamp_tdt)')
    parser.add_argument('-last', type=int, help='How many days back to pull data from.')
    parser.add_argument('-rows', type=int, default=500, help='Items per query. Default is 500')
    args = parser.parse_args()
    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)
    run()
    if not args.resume:
        args.resume=True
        run()
    logging.info("Processed {} Documents".format(GLOB['docs']))


