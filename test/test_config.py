test_config = {
    'SOLR_SERVER': ['http://localhost:7050/solr','http://localhost:7020/solr'],
    'SOLR_COLLECTION': 'SolrClient_unittest',
    'SOLR_CREDENTIALS': ('user','pass'),
    'collections':{
        'fields':[
            {'name':'product_name','stored':True,'indexed':True,'type':'text_en'},
            {'name':'product_name_exact','stored':True,'indexed':True,'type':'string'},
            {'name':'date','stored':True,'indexed':True,'type':'tdate'},
            {'name':'price','stored':True,'indexed':True,'type':'int'},
            {'name':'facet_test','stored':True,'indexed':True,'type':'string'},
            ],
        'copy_fields': [
            {'source':'product_name','dest':'product_name_exact'}
        ]},
    'docs': [],
}