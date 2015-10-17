import datetime
import logging
import sys
import os
import gzip
import shutil
import random
import json

class IndexQ():
    '''
    IndexQ sub module will help with indexing large amounts of content into Solr. It can be used to de-couple data processing with indexing. 
    
    For example, if you are working through a bunch of data that generates many small records that need to be updated you will need to either send them to Solr one at a time, or buffer them somewhere and possible combine multiple items into a larger update for solr. This is what this submodule is supposed to do for you. It uses an internal queue to buffer items and write them to the file system at certain increments. Then the indexer component can pick up these items and index them to Solr as a separate process. 
    
    Each queue is set up with the following directory structure
    queue_name/ 
     - todo/
     - done/ 
    
    Items get saved to the todo directory and once an item is processed it gets moved to the done directory. Items are also processed in chronological order. 
    '''

    def __init__(self, basepath, queue, compress=False, size=0, devel=False, threshold = 0.90,  mode='in', **kwargs ):
        '''
        :param string basepath: Path to the root of the indexQ. All other queues will get created underneath this. 
        :param string queue: Name of the queue. 
        :param string mode: If you are queuing (in) or de-queuing (out)
        :param bool compress: If todo files should be compressed, set to True if there is going to be a lot of data and these files will be sitting there for a while.
        :param int size: Internal buffer size (MB) that queued data must be to get written to the file system. If not passed, the data will be written to the filesystem as it is sent to IndexQ, otherwise they will be written when the buffer reaches 90%. 
        
        Example Usage::
            i = IndexQ('/data/indexq','parsed_data')
            
        '''
        self.logger = logging.getLogger(__package__)
        
        self._basepath = basepath
        self._queue_name = queue
        self._compress = compress
        self._size = size
        self._devel = devel
        self._threshold = threshold
        self._qpathdir = os.path.join(self._basepath,self._queue_name)
        self._todo_dir = os.path.join(self._basepath,self._queue_name,'todo')
        self._done_dir = os.path.join(self._basepath,self._queue_name,'done')
        self._mode = mode
        self._locked = False
        
        #Lock File        
        self._lck = self._qpathdir + 'index.lock'
        
        for dir in [self._qpathdir, self._todo_dir, self._done_dir]:
            if not os.path.isdir(dir):
                os.makedirs(dir)
        
        if self._mode == 'in':
            #First argument will be datestamp, second is counter
            self._output_filename_pattern = self._queue_name+"_{}.json"
            self._preprocess = self._buffer(self._size*1000000, self._write_file)
            
        elif self._mode == 'out':
            if self._lock():
                self._locked = True
                self.all_items = self._get_all_as_list()

        self.logger.info("Opening Queue {}".format(queue))
    
    #This part is all about loading data
    def _gen_file_name(self):
        '''
        Generates a random file name based on self._output_filename_pattern for the output to do file. 
        '''
        date = datetime.datetime.now()
        dt = "{}-{}-{}-{}-{}-{}-{}".format(str(date.year),str(date.month),str(date.day),str(date.hour),str(date.minute),str(date.second),str(random.randint(0,10000)))
        return self._output_filename_pattern.format(dt) 
            
    def add(self, item = None, finalize = False):
        '''
        Takes a string, dictionary or list of items for adding to queue. To help troubleshoot it will output the updated buffer size, however when the content gets written it will output the file path of the new file. Generally this can be safely discarded. 
        
        :param <dict,list> item: Item to add to the queue. If dict will be converted directly to a list and then to json. List must be a list of dictionaries. If a string is submitted, it will be written out as-is immediately and not buffered. 
        '''
        if item:
            if type(item) is list:
                if self._devel: self.logger.debug("Adding List")
                check = list(set([type(d) for d in item]))
                if len(check) > 1 or dict not in check:
                    raise ValueError("More than one data type detected in item (list). Make sure they are all dicts of data going to Solr")
            elif type(item) is dict:
                if self._devel: self.logger.debug("Adding Dict")
                item = [item]
            elif type(item) is str:
                if self._devel: self.logger.debug("Adding String")
                return self._write_file(item)
            else:
                raise ValueError("Not the right data submitted. Make sure you are sending a dict or list of dicts")
        return self._preprocess(item,finalize)

    def _write_file(self,content):
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

        
    def _buffer(self,size,callback):
        _c = {
            'size': 0,
            'callback': callback,
            'osize': size if size > 0 else 1,
            'buf': []
        }
        self.logger.debug("Starting Buffering Queue with Size of {}".format(size))
        def inner(item = None,finalize = False):
            if item:
                #Buffer Item
                [_c['buf'].append(x) for x in item]
                #Wish I didn't have to make a string of it over here sys.getsizeof wasn't providing accurate info either.
                _c['size'] += len(str(item))
                self.logger.debug("Item added to Buffer {} New Buffer Size is {}".format(self._queue_name, _c['size']))
            if _c['size'] / _c['osize'] > self._threshold or (finalize is True and len(_c['buf']) >= 1):
                #Write out the buffer
                if self._devel:
                    if finalize:
                        self.logger.debug("Finalize is True, writing out")
                    else:
                        self.logger.debug("Buffer Filled, writing out")
                res = _c['callback'](json.dumps(_c['buf'], indent=0, sort_keys=True))
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
        if os.path.isfile(self._lck):
            self.logger.error("Index already locked")
            return False
        else:
            with open(self.lck,'w') as fh:
                fh.write(str(os.getpid()))
            return True
        
    def _unlock(self):
        if self._devel: self.logger.debug("Unlocking Index")
        if os.path.isfile(self.lck):
            try:
                os.remove(self.lck)
                return True
            except:
                self.logger.error("Couldn't unlock - remove {}".format(self.lck))
                return False
        else:
            return True
        
    def _get_all_as_list(self):
        '''
        Returns a list of all items
        '''
        list = [x for x in os.listdir(self._todo_dir) if x.endswith('.json') or x.endswith('.json.gz')]
        full = [os.path.join(self._todo_dir,x) for x in list]
        full.sort(key=lambda x: os.path.getmtime(x))
        return full
    
    def complete(self,filename=False,**kwargs):
        '''
        Marks the item as complete; moves it to the done directory and compresses it. 
        '''
        if self.multi and filename:
            current = filename.replace(self.dirs['todo'],'')
        elif type(self.current) is str:
            current = self.current
        else:
            logging.error("Couldn't Complete, because current file was not understood")
            logging.error("CURRENT:".format(self.current))
            logging.error("FILENAME:".format(filename))
            return False
        logging.debug("Completing {} ".format(current))
        
        logging.debug("Moving {} to {}".format(self.dirs['todo']+current,self.dirs['done']+current))
        try:
            shutil.move(self.dirs['todo']+current,self.dirs['done']+'/'+current)
            self._compress(self.dirs['done']+current)
        except:
            try:
                shutil.move(self.dirs['todo']+current[:-3],self.dirs['done']+'/'+current)
                self._compress(self.dirs['done']+current[:-3])
            except:
                logging.error("Couldn't Complete the File")
        
        self.current = ''
