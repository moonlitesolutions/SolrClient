import logging
import json
import datetime
import random
import operator
import uuid
try:
    from .test_config import test_config
except SystemError:
    from test_config import test_config
except ImportError:
    from test_config import test_config
    
from time import sleep

class RandomTestData:
    '''
    Class for drumming up some test data for Solr based on the "collection" schema information in test_config
    '''
    def __init__(self):
        self.logger = logging.getLogger(__package__)
        self.fields = test_config['collections']['fields']
        self.fields.append({'name':'id','type':'id_string'})
        to_delete = []
        for i, field in enumerate(self.fields):
            if field['name'] == 'facet_test':
                field['type'] = 'facet_string'
            elif field['name'].endswith('exact'):
                to_delete.append(i)
        [self.fields.pop(i) for i in to_delete]
        self.lorem = '''
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus ac elementum lacus. Integer eu nulla vel nisi pretium posuere quis sit amet nulla. Nunc ornare consectetur quam a tristique. Vivamus vel vestibulum enim. Praesent mattis, lacus sit amet eleifend ultricies, ipsum nisi commodo elit, sed varius diam quam at dui. Praesent non aliquet nisi. Vestibulum bibendum auctor ligula. Vestibulum hendrerit quam vitae risus ullamcorper, in dapibus arcu fringilla. Vestibulum auctor tincidunt orci, at elementum libero tempus vel. Morbi imperdiet odio nunc, sit amet venenatis sem accumsan in. Mauris vulputate neque a ipsum fermentum mattis. In fringilla consectetur orci. Ut rutrum, enim accumsan tincidunt congue, diam dolor fermentum sapien, ac mattis nulla mauris non nisl. In auctor consectetur porttitor. Vivamus vel suscipit purus. Nunc lacinia ex vitae semper feugiat.
        '''
        self.lorem_words = self.lorem.split()
        self.date_signs = [operator.add,operator.sub]
        
    def _get_random_solr_date(self):
        date =  random.choice(self.date_signs)(datetime.datetime.now(),datetime.timedelta(days=random.random()*10))
        return date.utcnow().isoformat()+'Z'
        
    def _get_random_words(self,count=5):
        out = []
        for _ in range(count):
            out.append(random.choice(self.lorem_words))
        return out
    
    def _get_random_string(self,count=5):
        return " ".join(self._get_random_words())
    
    def _get_random_int(self,max=100):
        return int(random.uniform(1,max))
    
    def _get_id_string(self):
        return str(uuid.uuid4())    
    
    def _get_facet_string(self):
        return str(random.choice(self.lorem_words[:5]))
    
    def get_doc(self):
        doc = {}
        MAP = {
            'int': self._get_random_int,
            'tdate': self._get_random_solr_date,
            'text_en': self._get_random_string,
            'string': self._get_random_string,
            'id_string': self._get_id_string,
            'facet_string': self._get_facet_string
        }
        for field in self.fields:
            try:
                doc[field['name']] = MAP[field['type']]()
            except KeyError:
                doc[field['name']] = MAP['string']()
        return doc
    
    def get_docs(self,num_docs=50):
        docs = []
        for x in range(num_docs):
            docs.append(self.get_doc())
        return docs