import json
from .exceptions import *

class SolrResponse:
    def __init__(self, data):
        self.data = data
        self.query_time = data['responseHeader']['QTime']
        self.header = data['responseHeader']

        if 'response' in data:
            self.grouped = False
            self.docs = data['response']['docs']
            if 'numFound' in data['response']:
                self.num_found = data['response']['numFound']

        elif 'grouped' in data:
            self.groups = {}
            self.grouped = True
            for field in data['grouped']:
                #For backwards compatability
                self.groups = data['grouped'][field]['groups']
                self.docs = data['grouped'][field]['groups']
        else:
            self.grouped = False
            self.docs = {}


    def get_num_found(self):
        '''
        Returns number of documents found on an ungrounded query. ::

            >>> res = solr.query('SolrClient_unittest',{
                    'q':'*:*',
                    'facet':True,
                    'facet.field':'facet_test',
            })
            >>> res.get_num_found()
            50
        '''
        try:
            return self.num_found
        except AttributeError:
            raise AttributeError("num_found not found in response, make sure you aren't running this on a grouped query. ")


    def get_results_count(self):
        '''
        Returns the number of documents returned in current query. ::

            >>> res = solr.query('SolrClient_unittest',{
                'q':'*:*',
                'facet':True,
                'facet.field':'facet_test',
                })
            >>>
            >>> res.get_results_count()
            10
            >>> res.get_num_found()
            50
        '''
        return len(self.docs)


    def _determine_group_field(self, field=None):
        if not field:
            if len(list(self.data['grouped'].keys())) > 1:
                raise AttributeError("More than one grouped field in response, specify field to get count for. ")
            elif len(list(self.data['grouped'].keys())) == 1:
                field = list(self.data['grouped'].keys())[0]
            else:
                raise ValueError("Unable to determine what field to retrieve count for. Specify just to be sure.")
        return field


    def get_ngroups(self, field=None):
        '''
        Returns ngroups count if it was specified in the query, otherwise ValueError.

        If grouping on more than one field, provide the field argument to specify which count you are looking for.
        '''
        field = field if field else self._determine_group_field(field)
        if 'ngroups' in self.data['grouped'][field]:
            return self.data['grouped'][field]['ngroups']
        raise ValueError("ngroups not found in response. specify group.ngroups in the query.")


    def get_groups_count(self, field=None):
        '''
        Returns 'matches' from group response.

                If grouping on more than one field, provide the field argument to specify which count you are looking for.
        '''
        field = field if field else self._determine_group_field(field)
        if 'matches' in self.data['grouped'][field]:
            return self.data['grouped'][field]['matches']
        raise ValueError("group matches not found in response")


    def get_flat_groups(self, field=None):
        '''
        Flattens the group response and just returns a list of documents.
        '''
        field = field if field else self._determine_group_field(field)
        temp_groups = self.data['grouped'][field]['groups']
        return [y for x in temp_groups for y in x['doclist']['docs']]


    def get_facets(self):
        '''
        Returns a dictionary of facets::

            >>> res = solr.query('SolrClient_unittest',{
                    'q':'product_name:Lorem',
                    'facet':True,
                    'facet.field':'facet_test',
            })... ... ... ...
            >>> res.get_results_count()
            4
            >>> res.get_facets()
            {'facet_test': {'ipsum': 0, 'sit': 0, 'dolor': 2, 'amet,': 1, 'Lorem': 1}}

        '''
        if not hasattr(self,'facets'):
            self.facets = {}
            data = self.data
            if 'facet_counts' in data.keys() and type(data['facet_counts']) == dict:
                if 'facet_fields' in data['facet_counts'].keys() and type(data['facet_counts']['facet_fields']) == dict:
                    for facetfield in data['facet_counts']['facet_fields']:
                        if type(data['facet_counts']['facet_fields'][facetfield] == list):
                            l = data['facet_counts']['facet_fields'][facetfield]
                            self.facets[facetfield] = dict(zip(l[::2],l[1::2]))
                return self.facets
            else:
                raise SolrResponseError("No Facet Information in the Response")
        else:
            return self.facets

    def get_cursor(self):
        '''
        If you asked for the cursor in your query, this will return the next cursor mark.
        '''
        if 'nextCursorMark' in self.data:
            return self.data['nextCursorMark']
        else:
            raise SolrResponseError("No Cursor Mark in the Response")


    def get_facets_ranges(self):
        '''
        Returns query facet ranges ::

            >>> res = solr.query('SolrClient_unittest',{
                'q':'*:*',
                'facet':True,
                'facet.range':'price',
                'facet.range.start':0,
                'facet.range.end':100,
                'facet.range.gap':10
                })
            >>> res.get_facets_ranges()
            {'price': {'80': 9, '10': 5, '50': 3, '20': 7, '90': 3, '70': 4, '60': 7, '0': 3, '40': 5, '30': 4}}

        '''
        if not hasattr(self,'facet_ranges'):
            self.facet_ranges = {}
            data = self.data
            if 'facet_counts' in data.keys() and type(data['facet_counts']) == dict:
                if 'facet_ranges' in data['facet_counts'].keys() and type(data['facet_counts']['facet_ranges']) == dict:
                    for facetfield in data['facet_counts']['facet_ranges']:
                        if type(data['facet_counts']['facet_ranges'][facetfield]['counts']) == list:
                            l = data['facet_counts']['facet_ranges'][facetfield]['counts']
                            self.facet_ranges[facetfield] = dict(zip(l[::2],l[1::2]))
                    return self.facet_ranges
            else:
                raise SolrResponseError("No Facet Ranges in the Response")
        else:
            return self.facet_ranges


    def get_facet_pivot(self):
        '''
        Parses facet pivot response. Example::
            >>> res = solr.query('SolrClient_unittest',{
            'q':'*:*',
            'fq':'price:[50 TO *]',
            'facet':True,
            'facet.pivot':'facet_test,price' #Note how there is no space between fields. They are just separated by commas
            })
            >>> res.get_facet_pivot()
            {'facet_test,price': {'Lorem': {89: 1, 75: 1}, 'ipsum': {53: 1, 70: 1, 55: 1, 89: 1, 74: 1, 93: 1, 79: 1}, 'dolor': {61: 1, 94: 1}, 'sit': {99: 1, 50: 1, 67: 1, 52: 1, 54: 1, 71: 1, 72: 1, 84: 1, 62: 1}, 'amet,': {68: 1}}}

        This method has built in recursion and can support indefinite number of facets. However, note that the output format is significantly massaged since Solr by default outputs a list of fields in each pivot field.
        '''
        if not hasattr(self,'facet_pivot'):
            self.facet_pivot = {}
            if 'facet_counts' in self.data.keys():
                pivots = self.data['facet_counts']['facet_pivot']
                for fieldset in pivots:
                    self.facet_pivot[fieldset] = {}
                    for sub_field_set in pivots[fieldset]:
                        res = self._rec_subfield(sub_field_set)
                        self.facet_pivot[fieldset].update(res)
                return self.facet_pivot
        else:
            return self.facet_pivot

    def _rec_subfield(self,sub_field_set):
        out = {}
        if type(sub_field_set) is list:
            for set in sub_field_set:
                if 'pivot' in set.keys():
                    out[sub_field_set['value']] = self._rec_subfield(set['pivot'])
                else:
                    out[set['value']] = set['count']
        elif type(sub_field_set) is dict:
            if 'pivot' in sub_field_set:
                out[sub_field_set['value']] = self._rec_subfield(sub_field_set['pivot'])
        return out

    def get_field_values_as_list(self,field):
        '''
        :param str field: The name of the field for which to pull in values.
        Will parse the query results (must be ungrouped) and return all values of 'field' as a list. Note that these are not unique values.  Example::

            >>> r.get_field_values_as_list('product_name_exact')
            ['Mauris risus risus lacus. sit', 'dolor auctor Vivamus fringilla. vulputate', 'semper nisi lacus nulla sed', 'vel amet diam sed posuere', 'vitae neque ultricies, Phasellus ac', 'consectetur nisi orci, eu diam', 'sapien, nisi accumsan accumsan In', 'ligula. odio ipsum sit vel', 'tempus orci. elit, Ut nisl.', 'neque nisi Integer nisi Lorem']

        '''
        return [doc[field] for doc in self.docs if field in doc]

    #Not Sure what this one is doing or why I wrote it
    #Will find out later when migrating the rest of the code
    '''
    def get_fields_as_dict(self,field):
        out = {}
        for doc in self.docs:
            if field[0] in doc.keys() and field[1] in doc.keys():
                out[doc[field[0]]] = doc[field[1]]
        return out

    #Not Sure what this one is doing or why I wrote it
    def get_fields_as_list(self,field):
        out = []
        for doc in self.docs:
            t = []
            for f in field:
                if f in doc.keys():
                    t.append(doc[f])
                else:
                    t.append("")
            if len(t)> 0:
                out.append(t)
        return out

    #Not Sure what this one is doing or why I wrote it
    def get_multi_fields_as_dict(self,fields):
        out = {}
        for doc in self.docs:
            if fields[0] in doc.keys():
                out[doc[fields[0]]] = {}
                for field in fields[1:]:
                    if field in doc.keys():
                        out[doc[fields[0]]][field] = doc[field]
        return out
    '''


    def get_first_field_values_as_list(self, field):
        '''
        :param str field: The name of the field for lookup.

        Goes through all documents returned looking for specified field. At first encounter will return the field's value.
        '''
        for doc in self.docs:
            if field in doc.keys():
                return doc[field]
        raise SolrResponseError("No field in result set")

    def get_facet_values_as_list(self, field):
        '''
        :param str field: Name of facet field to retrieve values from.

        Returns facet values as list for a given field. Example::

            >>> res = solr.query('SolrClient_unittest',{
                'q':'*:*',
                'facet':'true',
                'facet.field':'facet_test',
            })
            >>> res.get_facet_values_as_list('facet_test')
            [9, 6, 14, 10, 11]
            >>> res.get_facets()
            {'facet_test': {'Lorem': 9, 'ipsum': 6, 'amet,': 14, 'dolor': 10, 'sit': 11}}

        '''
        facets = self.get_facets()
        out = []
        if field in facets.keys():
            for facetfield in facets[field]:
                out.append(facets[field][facetfield])
            return out
        else:
            raise SolrResponseError("No field in facet output")

    def get_facet_keys_as_list(self,field):
        '''
        :param str field: Name of facet field to retrieve keys from.

        Similar to get_facet_values_as_list but returns the list of keys as a list instead.
        Example::

            >>> r.get_facet_keys_as_list('facet_test')
            ['Lorem', 'ipsum', 'amet,', 'dolor', 'sit']

        '''
        facets = self.get_facets()
        if facets == -1:
            return facets
        if field in facets.keys():
            return [x for x in facets[field]]

    def get_json(self):
        '''
        Returns json from the original response.
        '''
        return json.dumps(self.data)


    def json_facet(self, field=None):
        '''
        EXPERIMENTAL

        Tried to kick back the json.fact output.
        '''
        facets = self.data['facets']
        if field is None:
            temp_fields = [x for x in facets.keys() if x != 'count']
            if len(temp_fields) != 1:
                raise ValueError("field argument not specified and it looks like there is more than one field in facets. Specify the field to get json.facet from. ")
            field = temp_fields[0]

        if field not in self.data['facets']:
            raise ValueError("Facet Field {} Not found in response, available fields are {}".format(
                                        field, self.data['facets'].keys() ))
        return self.data['facets'][field]

    def get_jsonfacet_counts_as_dict(self, field, data=None):
        '''
        EXPERIMENTAL
        Takes facets and returns then as a dictionary that is easier to work with,
        for example, if you are getting something this::

            {'facets': {'count': 50,
              'test': {'buckets': [{'count': 10,
                 'pr': {'buckets': [{'count': 2, 'unique': 1, 'val': 79},
                   {'count': 1, 'unique': 1, 'val': 9}]},
                 'pr_sum': 639.0,
                 'val': 'consectetur'},
                {'count': 8,
                 'pr': {'buckets': [{'count': 1, 'unique': 1, 'val': 9},
                   {'count': 1, 'unique': 1, 'val': 31},
                   {'count': 1, 'unique': 1, 'val': 33}]},
                 'pr_sum': 420.0,
                 'val': 'auctor'},
                {'count': 8,
                 'pr': {'buckets': [{'count': 2, 'unique': 1, 'val': 94},
                   {'count': 1, 'unique': 1, 'val': 25}]},
                 'pr_sum': 501.0,
                 'val': 'nulla'}]}}}


        This should return you something like this::

            {'test': {'auctor': {'count': 8,
                                 'pr': {9: {'count': 1, 'unique': 1},
                                        31: {'count': 1, 'unique': 1},
                                        33: {'count': 1, 'unique': 1}},
                                 'pr_sum': 420.0},
                      'consectetur': {'count': 10,
                                      'pr': {9: {'count': 1, 'unique': 1},
                                             79: {'count': 2, 'unique': 1}},
                                      'pr_sum': 639.0},
                      'nulla': {'count': 8,
                                'pr': {25: {'count': 1, 'unique': 1},
                                       94: {'count': 2, 'unique': 1}},
                                'pr_sum': 501.0}}}
        '''
        data = data if data else self.data['facets']
        if field not in data:
            raise ValueError("Field To start Faceting on not specified.")
        out = { field: self._json_rec_dict(data[field]['buckets']) }
        return out


    def _json_rec_dict(self, buckets):
        out = {}
        for bucket in buckets:
            out[bucket['val']] = {}
            out[bucket['val']]['count'] = bucket['count']
            for field in [x for x in bucket if x not in ['val']]:
                if type(bucket[field]) is dict and 'buckets' in bucket[field]:
                    out[bucket['val']][field] = self._json_rec_dict(bucket[field]['buckets'])
                else:
                    out[bucket['val']][field] = bucket[field]
        return out
