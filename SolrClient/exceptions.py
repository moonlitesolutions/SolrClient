class SolrError(Exception):
    """
    Class to handle any issues that Solr Reports
    """


class SolrResponseError(SolrError):
    """
    Errors relatd to parsing Solr Response
    """


class ConnectionError(SolrError):
    """
    Errors connecting to Solr
    """


class ZookeeperError(SolrError):
    """
    Errors connecting to Zookeeper
    """


class NotFoundError(SolrError):
    """
    When a document wasn't found
    """


class MinRfError(SolrError):
    """
    When an index request didn't satisfy the required min_rf
    """
    rf = None
    min_rf = None

    def __init__(self, message, rf, min_rf, **kwargs):
        self.rf = rf
        self.min_rf = min_rf
        super().__init__(self, message, **kwargs)
