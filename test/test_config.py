import os

test_config = {
    'SOLR_SERVER': [os.environ.get('SOLR_TEST_URL')],
    'SOLR_COLLECTION': 'SolrClient_unittest',
    'temp_data': os.getcwd()+os.sep+'temp',
    'SOLR_REINDEXER_COLLECTION_S': 'source_coll',
    'SOLR_REINDEXER_COLLECTION_D': 'dest_coll',
    'SOLR_CREDENTIALS': (os.environ.get('SOLR_TEST_USER'), os.environ.get('SOLR_TEST_PASS')),
    'collections':{
        'fields':[
            {'name': 'product_name', 'stored': True, 'indexed': True, 'type':'text_en'},
            {'name':'product_name_exact','stored':True,'indexed':True,'type':'string'},
            {'name':'product_name_s'},
            {'name':'date','stored':True,'indexed':True,'type':'tdate'},
            {'name':'price','stored':True,'indexed':True,'type':'int'},
            {'name':'facet_test','stored':True,'indexed':True,'type':'string'},
            {'name':'facet_test_s'},
            ],
        'copy_fields': [
            {'source':'product_name','dest':'product_name_exact'},
            {'source':'product_name','dest':'product_name_s'},
            {'source':'facet_test','dest':'facet_test_s'},
        ]},
    'docs': [],
}
if __name__=='__main__':
    pass
