
class SolrError(Exception):
    """
    Class to handle any issues that Solr Reports
    """
    
class SolrResponseError(Exception):
    '''
    Errors relatd to parsing Solr Response
    '''
    
class ConnectionError(Exception):
    '''
    Errors connecting to Solr
    '''