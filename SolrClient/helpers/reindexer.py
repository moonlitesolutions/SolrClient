import datetime
import logging
import sys
import gzip
import argparse
import os
import json
from datetime import datetime, timedelta
from time import time, sleep
from SolrClient import SolrClient, IndexQ


class Reindexer():
    '''
    Initiates the re-indexer.

    :param source: An instance of SolrClient.
    :param dest: An instance of SolrClient or an instance of IndexQ.
    :param string source_coll: Source collection name.
    :param string dest_coll: Destination collection name; only required if destination is SolrClient.
    :param int rows: Number of items to get in each query; default is 1000, however you will probably want to increase it.
    :param string date_field: String name of a Solr date field to use in sort and resume.
    :param bool devel: Whenever to turn on super verbouse logging for development. Standard DEBUG should suffice for most developemnt.
    :param bool per_shard: Will add distrib=false to each query to get the data. Use this only if you will be running multiple instances of this to get the rest of the shards.
    :param list ignore_fields: What fields to exclude from Solr queries. This is important since if you pull them out, you won't be able to index the documents in.
    By default, it will try to determine and exclude copy fields as well as _version_. Pass in your own list to override or set it to False to prevent it from doing anything.
    '''
    def __init__(self,
                source,
                dest,
                source_coll=None,
                dest_coll=None,
                rows=1000,
                date_field=None,
                devel=False,
                per_shard=False,
                ignore_fields=['_version_'],
                ):


        self.log = logging.getLogger('reindexer')

        self._source = source
        self._source_coll = source_coll
        self._dest = dest
        self._dest_coll = dest_coll
        self._rows = rows
        self._date_field = date_field
        self._per_shard = per_shard
        self._items_processed = 0
        self._devel = devel
        self._ignore_fields = ignore_fields


        #Determine what source and destination should be
        if type(source) is SolrClient and source_coll:
            self._getter = self._from_solr
             #Maybe break this out later for the sake of testing
            if type(self._ignore_fields) is list and len(self._ignore_fields) == 1:
                self._ignore_fields.extend(self._get_copy_fields())

        elif type(source) is str and os.path.isdir(source):
            self._getter = self._from_json
        else:
            raise ValueError("Incorrect Source Specified. Pass either a directory with json files or source SolrClient \
                            instance with the name of the collection.")

        if type(self._dest) is SolrClient and self._dest_coll:
            self._putter = self._to_solr
        elif type(dest) is IndexQ:
            self._putter = self._to_IndexQ
        else:
           raise ValueError("Incorrect Destination Specified. Pass either a directory with json files or destination SolrClient \
                            instance with the name of the collection.")
        self.log.info("Reindexer created succesfully. ")


    def _get_copy_fields(self):
        if self._devel:
            self.log.debug("Getting additional copy fields to exclude")
            self.log.debug(self._source.schema.get_schema_copyfields(self._source_coll))
        fields =  [field['dest'] for field in self._source.schema.get_schema_copyfields(self._source_coll)]
        self.log.info("Field exclusions are: {}".format(", ".join(fields)))
        return fields


    def reindex(self, fq= [], **kwargs):
        '''
        Starts Reindexing Process. All parameter arguments will be passed down to the getter function.
        :param string fq: FilterQuery to pass to source Solr to retrieve items. This can be used to limit the results.
        '''
        for items in self._getter(fq=fq, **kwargs):
            self._putter(items)
        if type(self._dest) is SolrClient and self._dest_coll:
            self.log.info("Finished Indexing, sending a commit")
            self._dest.commit(self._dest_coll, openSearcher=True)


    def _from_solr(self, fq=[], report_frequency = 25):
        '''
        Method for retrieving batch data from Solr.
        '''
        cursor = '*'
        stime = datetime.now()
        query_count = 0
        while True:
            #Get data with starting cursorMark
            query = self._get_query(cursor)
            #Add FQ to the query. This is used by resume to filter on date fields and when specifying document subset.
            #Not included in _get_query for more flexibiilty.

            if fq:
                if 'fq' in query:
                    [query['fq'].append(x) for x in fq]
                else:
                    query['fq'] = fq

            results = self._source.query(self._source_coll, query)
            query_count += 1
            if query_count % report_frequency == 0:
                self.log.info("Processed {} Items in {} Seconds. Apprximately {} items/minute".format(
                            self._items_processed, int((datetime.now()-stime).seconds),
                            str(int(self._items_processed / ((datetime.now()-stime).seconds/60)))
                            ))

            if results.get_results_count():
                #If we got items back, get the new cursor and yield the docs
                self._items_processed += results.get_results_count()
                cursor = results.get_cursor()
                #Remove ignore fields
                docs = self._trim_fields(results.docs)
                yield docs
                if results.get_results_count() < self._rows:
                    #Less results than asked, probably done
                    break
            else:
                #No Results, probably done :)
                self.log.debug("Got zero Results with cursor: {}".format(cursor))
                break


    def _trim_fields(self, docs):
        '''
        Removes ignore fields from the data that we got from Solr.
        '''
        for doc in docs:
            for field in self._ignore_fields:
                if field in doc:
                    del(doc[field])
        return docs


    def _get_query(self, cursor):
        '''
        Query tempalte for source Solr, sorts by id by default.
        '''
        query = {'q':'*:*',
                'sort':'id desc',
                'rows':self._rows,
                'cursorMark':cursor}
        if self._date_field:
            query['sort'] = "{} asc, id desc".format(self._date_field)
        if self._per_shard:
            query['distrib'] = 'false'
        return query


    def _to_IndexQ(self, data):
        '''
        Sends data to IndexQ instance.
        '''
        self._dest.add(data)


    def _to_solr(self, data):
        '''
        Sends data to a Solr instance.
        '''
        return self._dest.index_json(self._dest_coll, json.dumps(data,sort_keys=True))


    def _get_date_range_query(self, start_date, end_date, timespan= 'DAY', date_field= None):
        '''
        Gets counts of items per specified date range.
        :param collection: Solr Collection to use.
        :param timespan: Solr Date Math compliant value for faceting ex HOUR, MONTH, DAY
        '''
        if date_field is None:
            date_field = self._date_field
        query ={'q':'*:*',
                'rows':0,
                'facet':'true',
                'facet.range': date_field,
                'facet.range.gap': '+1{}'.format(timespan),
                'facet.range.end': '{}'.format(end_date),
                'facet.range.start': '{}'.format(start_date),
                'facet.range.include': 'all'
                }
        if self._per_shard:
            query['distrib'] = 'false'
        return query


    def _get_edge_date(self, date_field, sort):
        '''
        This method is used to get start and end dates for the collection.
        '''
        return self._source.query(self._source_coll, {
                'q':'*:*',
                'rows':1,
                'fq':'+{}:*'.format(date_field),
                'sort':'{} {}'.format(date_field, sort)}).docs[0][date_field]


    def _get_date_facet_counts(self, timespan, date_field, start_date=None, end_date=None):
        '''
        Returns Range Facet counts based on
        '''
        if 'DAY' not in timespan:
            raise ValueError("At this time, only DAY date range increment is supported. Aborting..... ")

        #Need to do this a bit better later. Don't like the string and date concatenations.
        if not start_date:
            start_date = self._get_edge_date(date_field, 'asc')
            start_date = datetime.strptime(start_date,'%Y-%m-%dT%H:%M:%S.%fZ').date().isoformat()+'T00:00:00.000Z'
        else:
            start_date = start_date+'T00:00:00.000Z'

        if not end_date:
            end_date = self._get_edge_date(date_field, 'desc')
            end_date = datetime.strptime(end_date,'%Y-%m-%dT%H:%M:%S.%fZ').date()
            end_date += timedelta(days=1)
            end_date = end_date.isoformat()+'T00:00:00.000Z'
        else:
            end_date = end_date+'T00:00:00.000Z'


        self.log.info("Processing Items from {} to {}".format(start_date, end_date))

        #Get facet counts for source and destination collections
        source_facet = self._source.query(self._source_coll,
            self._get_date_range_query(timespan=timespan, start_date=start_date, end_date=end_date)
            ).get_facets_ranges()[date_field]
        dest_facet = self._dest.query(
            self._dest_coll, self._get_date_range_query(
                    timespan=timespan, start_date=start_date, end_date=end_date
                    )).get_facets_ranges()[date_field]
        return source_facet, dest_facet


    def resume(self, start_date=None, end_date=None, timespan='DAY', check= False):
        '''
        This method may help if the original run was interrupted for some reason. It will only work under the following conditions
        * You have a date field that you can facet on
        * Indexing was stopped for the duration of the copy

        The way this tries to resume re-indexing is by running a date range facet on the source and destination collections. It then compares
        the counts in both collections for each timespan specified. If the counts are different, it will re-index items for each range where
        the counts are off. You can also pass in a start_date to only get items after a certain time period. Note that each date range will be indexed in
        it's entirety, even if there is only one item missing.

        Keep in mind this only checks the counts and not actual data. So make the indexes weren't modified between the reindexing execution and
        running the resume operation.

        :param start_date: Date to start indexing from. If not specified there will be no restrictions and all data will be processed. Note that
        this value will be passed to Solr directly and not modified.
        :param end_date: The date to index items up to. Solr Date Math compliant value for faceting; currenlty only DAY is supported.
        :param timespan: Solr Date Math compliant value for faceting; currenlty only DAY is supported.
        :param check: If set to True it will only log differences between the two collections without actually modifying the destination.
        '''

        if type(self._source) is not SolrClient or type(self._dest) is not SolrClient:
            raise ValueError("To resume, both source and destination need to be Solr.")

        source_facet, dest_facet = self._get_date_facet_counts(timespan, self._date_field, start_date=start_date, end_date=end_date)

        for dt_range in sorted(source_facet):
            if dt_range in dest_facet:
                self.log.info("Date Range: {} Source: {} Destination:{} Difference:{}".format(
                        dt_range, source_facet[dt_range], dest_facet[dt_range], (source_facet[dt_range]-dest_facet[dt_range])))
                if check:
                    continue
                if source_facet[dt_range] > dest_facet[dt_range]:
                    #Kicks off reindexing with an additional FQ
                    self.reindex(fq=['{}:[{} TO {}]'.format(self._date_field, dt_range, dt_range+'+1{}'.format(timespan))])
                    self.log.info("Complete Date Range {}".format(dt_range))
            else:
                self.log.error("Something went wrong; destinationSource: {}".format(source_facet))
                self.log.error("Destination: {}".format(dest_facet))
                raise ValueError("Date Ranges don't match up")
        self._dest.commit(self._dest_coll, openSearcher=True)
