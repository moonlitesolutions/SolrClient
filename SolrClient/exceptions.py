class SolrError(Exception):
    """
    Class to handle any issues that Solr Reports
    """


class SolrResponseError(Exception):
    """
    Errors relatd to parsing Solr Response
    """


class ConnectionError(Exception):
    """
    Errors connecting to Solr
    """


class ZookeeperError(Exception):
    """
    Errors connecting to Zookeeper
    """


class NotFoundError(Exception):
    """
    When a document wasn't found
    """
