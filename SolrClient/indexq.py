import datetime
import logging
import sys
import os
import gzip
import shutil
import random
import json
import threading
import time
from multiprocessing.pool import ThreadPool
from multiprocessing import Process, JoinableQueue
from functools import partial
from SolrClient.exceptions import *




class IndexQ():
    '''
    IndexQ sub module will help with indexing content into Solr. It can be used to de-couple data processing from indexing.

    Each queue is set up with the following directory structure
    queue_name/
     - todo/
     - done/

    Items get saved to the todo directory and once an item is processed it gets moved to the done directory. Items are processed in chronological order.
    '''

    def __init__(self, basepath, queue, compress=False, compress_complete=False, size=0, devel=False,
                 threshold=0.90, log=None, rotate_complete=None, **kwargs ):
        '''
        :param string basepath: Path to the root of the indexQ. All other queues will get created underneath this.
        :param string queue: Name of the queue.
        :param log: Logging instance that you want it to log to.
        :param rotate_complete: Supply a callable that will be used to store completed files. Completed files will be moved to /done/`callable_output`/.
        :param bool compress: If todo files should be compressed, set to True if there is going to be a lot of data and these files will be sitting there for a while.
        :param bool compress: If done files should be compressed, set to True if there is going to be a lot of data and these files will be sitting there for a while.
        :param int size: Internal buffer size (MB) that queued data must be to get written to the file system. If not passed, the data will be written to the filesystem as it is sent to IndexQ, otherwise they will be written when the buffer reaches 90%.

        Example Usage::
            i = IndexQ('/data/indexq','parsed_data')

        '''
        self.logger = log or logging.getLogger(__package__)

        self._basepath = basepath
        self._queue_name = queue
        self._compress = compress
        self._compress_complete = compress_complete
        self._size = size
        self._devel = devel
        self._threshold = threshold
        self._qpathdir = os.path.join(self._basepath, self._queue_name)
        self._todo_dir = os.path.join(self._basepath, self._queue_name, 'todo')
        self._done_dir = os.path.join(self._basepath, self._queue_name, 'done')
        self._locked = False
        self._rlock = threading.RLock()
        self.rotate_complete = rotate_complete
        #Lock File
        self._lck = os.path.join(self._qpathdir,'index.lock')

        for dir in [self._qpathdir, self._todo_dir, self._done_dir]:
            if not os.path.isdir(dir):
                os.makedirs(dir)

        #First argument will be datestamp, second is counter
        self._output_filename_pattern = self._queue_name+"_{}.json"
        self._preprocess = self._buffer(self._size*1000000, self._write_file)
        self.logger.info("Opening Queue {}".format(queue))


    def _gen_file_name(self):
        '''
        Generates a random file name based on self._output_filename_pattern for the output to do file.
        '''
        date = datetime.datetime.now()
        dt = "{}-{}-{}-{}-{}-{}-{}".format(str(date.year),str(date.month),str(date.day),str(date.hour),str(date.minute),str(date.second),str(random.randint(0,10000)))
        return self._output_filename_pattern.format(dt)


    def add(self, item=None, finalize=False, callback=None):
        '''
        Takes a string, dictionary or list of items for adding to queue. To help troubleshoot it will output the updated buffer size, however when the content gets written it will output the file path of the new file. Generally this can be safely discarded.

        :param <dict,list> item: Item to add to the queue. If dict will be converted directly to a list and then to json. List must be a list of dictionaries. If a string is submitted, it will be written out as-is immediately and not buffered.
        :param bool finalize: If items are buffered internally, it will flush them to disk and return the file name.
        :param callback: A callback function that will be called when the item gets written to disk. It will be passed one position argument, the file path of the file written. Note that errors from the callback method will not be re-raised here.
        '''
        if item:
            if type(item) is list:
                check = list(set([type(d) for d in item]))
                if len(check) > 1 or dict not in check:
                    raise ValueError("More than one data type detected in item (list). Make sure they are all dicts of data going to Solr")
            elif type(item) is dict:
                item = [item]
            elif type(item) is str:
                return self._write_file(item)
            else:
                raise ValueError("Not the right data submitted. Make sure you are sending a dict or list of dicts")
        with self._rlock:
            res = self._preprocess(item, finalize, callback)
        return res


    def _write_file(self, content):
        while True:
            path = os.path.join(self._todo_dir,self._gen_file_name())
            if self._compress:
                path += '.gz'
            if not os.path.isfile(path):
                break
        self.logger.info("Writing new file to {}".format(path))
        if self._compress:
            with gzip.open(path, 'wb') as f:
                f.write(content.encode('utf-8'))
        else:
            with open(path,'w') as f:
                f.write(content)
        return path


    def _buffer(self, size, callback):
        _c = {
            'size': 0,
            'callback': callback,
            'osize': size if size > 0 else 1,
            'buf': []
        }
        self.logger.debug("Starting Buffering Queue with Size of {}".format(size))
        def inner(item=None, finalize=False, listener=None):
            #Listener is the external callback specific by the user. Need to change the names later a bit.
            if item:
                #Buffer Item
                [_c['buf'].append(x) for x in item]
                #Wish I didn't have to make a string of it over here sys.getsizeof wasn't providing accurate info either.
                _c['size'] += len(str(item))
                if self._devel:
                    self.logger.debug("Item added to Buffer {} New Buffer Size is {}".format(self._queue_name, _c['size']))
            if _c['size'] / _c['osize'] > self._threshold or (finalize is True and len(_c['buf']) >= 1):
                #Write out the buffer
                if self._devel:
                    if finalize:
                        self.logger.debug("Finalize is True, writing out")
                    else:
                        self.logger.debug("Buffer Filled, writing out")
                res = _c['callback'](json.dumps(_c['buf'], indent=0, sort_keys=True))
                if listener:
                    try:
                        listener(res)
                    except Exception as e:
                        self.logger.error("Problems in the Callback specified")
                        self.logger.exception(e)
                if res:
                    _c['buf'] = []
                    _c['size'] = 0
                    return res
                else:
                    raise RuntimeError("Couldn't write out the buffer." + _c)
            return _c['size']
        return inner



    #This is about pullind data out
    def _lock(self):
        '''
        Locks, or returns False if already locked
        '''
        if not self._is_locked():
            with open(self._lck,'w') as fh:
                if self._devel: self.logger.debug("Locking")
                fh.write(str(os.getpid()))
            return True
        else:
            return False


    def _is_locked(self):
        '''
        Checks to see if we are already pulling items from the queue
        '''
        if os.path.isfile(self._lck):
            try:
                import psutil
            except ImportError:
                return True #Lock file exists and no psutil
            #If psutil is imported
            with open(self._lck) as f:
                pid = f.read()
            return True if psutil.pid_exists(int(pid)) else False
        else:
            return False


    def _unlock(self):
        '''
        Unlocks the index
        '''
        if self._devel: self.logger.debug("Unlocking Index")
        if self._is_locked():
            os.remove(self._lck)
            return True
        else:
            return True


    def get_all_as_list(self, dir='_todo_dir'):
        '''
        Returns a list of the the full path to all items currently in the todo directory. The items will be listed in ascending order based on filesystem time.
        This will re-scan the directory on each execution.

        Do not use this to process items, this method should only be used for troubleshooting or something axillary. To process items use get_todo_items() iterator.
        '''
        dir = getattr(self,dir)
        list = [x for x in os.listdir(dir) if x.endswith('.json') or x.endswith('.json.gz')]
        full = [os.path.join(dir,x) for x in list]
        full.sort(key=lambda x: os.path.getmtime(x))
        return full


    def get_todo_items(self, **kwargs):
        '''
        Returns an iterator that will provide each item in the todo queue. Note that to complete each item you have to run complete method with the output of this iterator.

        That will move the item to the done directory and prevent it from being retrieved in the future.
        '''
        def inner(self):
            for item in self.get_all_as_list():
                yield item
            self._unlock()

        if not self._is_locked():
            if self._lock():
                return inner(self)
        raise RuntimeError("RuntimeError: Index Already Locked")


    def complete(self, filepath):
        '''
        Marks the item as complete by moving it to the done directory and optionally gzipping it.
        '''
        if not os.path.exists(filepath):
            raise FileNotFoundError("Can't Complete {}, it doesn't exist".format(filepath))
        if self._devel: self.logger.debug("Completing - {} ".format(filepath))
        if self.rotate_complete:
            try:
                complete_dir = str(self.rotate_complete())
            except Exception as e:
                self.logger.error("rotate_complete function failed with the following exception.")
                self.logger.exception(e)
                raise
            newdir = os.path.join(self._done_dir, complete_dir)
            newpath = os.path.join(newdir, os.path.split(filepath)[-1] )

            if not os.path.isdir(newdir):
                self.logger.debug("Making new directory: {}".format(newdir))
                os.makedirs(newdir)
        else:
            newpath = os.path.join(self._done_dir, os.path.split(filepath)[-1] )

        try:
            if self._compress_complete:
                if not filepath.endswith('.gz'):
                    #  Compressing complete, but existing file not compressed
                    #  Compress and move it and kick out
                    newpath += '.gz'
                    self._compress_and_move(filepath, newpath)
                    return newpath
                # else the file is already compressed and can just be moved
            #if not compressing completed file, just move it
            shutil.move(filepath, newpath)
            self.logger.info(" Completed - {}".format(filepath))
        except Exception as e:
            self.logger.error("Couldn't Complete {}".format(filepath))
            self.logger.exception(e)
            raise
        return newpath


    def _compress_and_move(self, source, destination):
        try:
            self.logger.debug("Compressing and Moving Completed file: {} -> {}".format(source, destination))
            with gzip.open(destination, 'wb') as df, open(source, 'rb') as sf:
                    df.writelines(sf)
            os.remove(source)
        except Exception as e:
            self.logger.error("Unable to Compress and Move file {} -> {}".format(source, destination))
            self.logger.exception(e)
            raise
        return True


    def index(self, solr, collection, threads=1, send_method='stream_file', **kwargs):
        '''
        Will index the queue into a specified solr instance and collection. Specify multiple threads to make this faster, however keep in mind that if you specify multiple threads the items may not be in order.
        Example::
            solr = SolrClient('http://localhost:8983/solr/')
            for doc in self.docs:
                index.add(doc, finalize=True)
            index.index(solr,'SolrClient_unittest')

        :param object solr: SolrClient object.
        :param string collection: The name of the collection to index document into.
        :param int threads: Number of simultaneous threads to spin up for indexing.
        :param string send_method: SolrClient method to execute for indexing. Default is stream_file
        '''

        try:
            method = getattr(solr, send_method)
        except AttributeError:
            raise AttributeError("Couldn't find the send_method. Specify either stream_file or local_index")

        self.logger.info("Indexing {} into {} using {}".format(self._queue_name,
                                                               collection,
                                                               send_method))
        if threads > 1:
            if hasattr(collection, '__call__'):
                self.logger.debug("Overwriting send_method to index_json")
                method = getattr(solr, 'index_json')
                method = partial(self._wrap_dynamic, method, collection)
            else:
                method = partial(self._wrap, method, collection)
            with ThreadPool(threads) as p:
                p.map(method, self.get_todo_items())
        else:
            for todo_file in self.get_todo_items():
                try:
                    result = method(collection, todo_file)
                    if result:
                        self.complete(todo_file)
                except SolrError:
                    self.logger.error("Error Indexing Item: {}".format(todo_file))
                    self._unlock()
                    raise

    def _wrap(self, method, collection, doc):
        #Indexes entire file into the collection
        try:
            res = method(collection, doc)
            if res:
                self.complete(doc)
            return res
        except SolrError:
            self.logger.error("Error Indexing Item: {}".format(doc))
            pass

    def _wrap_dynamic(self, method, collection, doc):
        # Reads the file, executing 'collection' function on each item to
        # get the name of collection it should be indexed into
        try:
            j_data = self._open_file(doc)
            temp = {}
            for item in j_data:
                try:
                    coll = collection(item)
                    if coll in temp:
                        temp[coll].append(item)
                    else:
                        temp[coll] = [item]
                except Exception as e:
                    self.logger.error("Exception caught on dynamic collection function")
                    self.logger.error(item)
                    self.logger.exception(e)
                    raise

            indexing_errors = 0
            done = []
            for coll in temp:
                try:
                    res = method(coll, json.dumps(temp[coll]))
                    if res:
                        done.append(coll)
                except Exception as e:
                    self.logger.error("Indexing {} items into {} failed".format(len(temp[coll]), coll))
                    indexing_errors += 1
            if len(done) == len(temp.keys()) and indexing_errors == 0:
                self.complete(doc)
                return True
            return False

        except SolrError as e:
            self.logger.error("Error Indexing Item: {}".format(doc))
            self.logger.exception(e)
            pass


    def get_all_json_from_indexq(self):
        '''
        Gets all data from the todo files in indexq and returns one huge list of all data.
        '''
        files = self.get_all_as_list()
        out = []
        for efile in files:
            out.extend(self._open_file(efile))
        return out

    def _open_file(self, efile):
        if efile.endswith('.gz'):
            f = gzip.open(efile, 'rt', encoding='utf-8')
        else:
            f = open(efile)
        f_data = json.load(f)
        f.close()
        return f_data

    def get_multi_q(self, sentinel='STOP'):
        '''
        This helps indexq operate in multiprocessing environment without each process having to have it's own IndexQ. It also is a handy way to deal with thread / process safety.

        This method will create and return a JoinableQueue object. Additionally, it will kick off a back end process that will monitor the queue, de-queue items and add them to this indexq.

        The returned JoinableQueue object can be safely passed to multiple worker processes to populate it with data.

        To indicate that you are done writing the data to the queue, pass in the sentinel value ('STOP' by default).

        Make sure you call join_indexer() after you are done to close out the queue and join the worker.
        '''
        self.in_q = JoinableQueue()
        self.indexer_process = Process(target=self._indexer_process, args=(self.in_q, sentinel))
        self.indexer_process.daemon = False
        self.indexer_process.start()
        return self.in_q


    def join_indexer(self):
        self.logger.info("Joining Queue")
        self.in_q.join()
        self.logger.info("Joining Index Process")
        self.indexer_process.join()


    def _indexer_process(self, in_q, sentinel):
        self.logger.info("Indexing Process Started")
        count = 0
        total = 0
        stime = time.time()
        seen_STOP = False
        while True:
            if seen_STOP and in_q.qsize() == 0:
                #If sentinel has been seen and the queue size is zero, write out the queue and return.
                self.logger.info("Indexer Queue is empty. Stopping....")
                self.add(finalize=True)
                return

            if in_q.qsize() < 1 and not seen_STOP:
                #If there is nothing to do, just hang out for a few seconds
                time.sleep(3)
                continue

            item = in_q.get()
            if item == sentinel:
                self.logger.info("Indexer Received Stop Command, stopping indexer. Queue size is {}".format(str(in_q.qsize())))
                seen_STOP = True
                in_q.task_done()
                continue
            count += 1
            total += 1
            self.add(item)
            in_q.task_done()

            if (time.time() - stime) > 60:
                self.logger.debug("Indexed {} items in the last 60 seconds. Total: ".format(count, total))
                count = 0
                stime = time.time()
