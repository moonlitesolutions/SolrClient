#Testing Info

The unit tests are set-up to run against a real Solr instance that is public, but password protected. The credentials are stored privately in Travis and will be used for all pull requests and commits. All tests run against that instance except for the re-indexer tests which require SolrCloud. 

However, if you want to set-up your own environment chehck out the test_config.py file for the settings that all unit tests use. 

	test_config = {
	    'SOLR_SERVER': [os.environ.get('SOLR_TEST_URL')], #Pulls from ENV variables, but you can override it here
	    'SOLR_COLLECTION': 'SolrClient_unittest', # data managed type collection for most unit tests
	    'SOLR_REINDEXER_COLLECTION_S': 'source_coll', #Source collection for reindexer tests
	    'SOLR_REINDEXER_COLLECTION_D': 'dest_coll', #Destination collection for reindexer tests
	    'SOLR_CREDENTIALS': (os.environ.get('SOLR_TEST_USER'), os.environ.get('SOLR_TEST_PASS')), #Solr credentials
	    'collections':{ #This section lists the fields that are used in random data generation as well as in collection tests to make fields. 				  #This data will be passed to Solr exactly, so make sure it is correct. 
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

For the reindexer collections use the schema in the resources folder. 
