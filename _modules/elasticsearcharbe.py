# -*- coding: utf-8 -*-
'''
Connection module for Elasticsearch

notice: early state, etc.

:depends: elasticsearch
'''
# TODO
# * improve error/ exception handling

from __future__ import absolute_import

# Import Python libs
import logging

log = logging.getLogger(__name__)

# Import third party libs
try:
    import elasticsearch
    logging.getLogger('elasticsearch').setLevel(logging.CRITICAL)
    HAS_ELASTICSEARCH = True
except ImportError:
    HAS_ELASTICSEARCH = False

from salt.ext.six import string_types


def __virtual__():
    '''
    Only load if elasticsearch libraries exist.
    '''
    if not HAS_ELASTICSEARCH:
        return False
    return True


def index_create(index, body={}, hosts=None, profile='elasticsearch'):
    '''
    Create an index in Elasticsearch

    CLI example::

        salt myminion elasticsearch.index_create testindex
    '''
    es = _get_instance(hosts, profile)
    try:
        if index_exists(index):
            return True
        else:
            result = es.indices.create(index=index, body=body) # TODO error handling
            return index_get(index)
    except elasticsearch.exceptions.NotFoundError:
        return False
    return False


def index_delete(index, hosts=None, profile='elasticsearch'):
    '''
    Delete an index in Elasticsearch

    CLI example::

        salt myminion elasticsearch.index_delete testindex
    '''
    es = _get_instance(hosts, profile)
    try:
        if not index_exists(index=index):
            return True
        else:
            result = es.indices.delete(index=index)

            if result.get('acknowledged', False): # TODO error handling
                return True
    except elasticsearch.exceptions.NotFoundError:
        return False
    return False


def index_exists(index, hosts=None, profile='elasticsearch'):
    '''
    Return a boolean indicating whether given index exists

    CLI example::

        salt myminion elasticsearch.index_exists testindex
    '''
    es = _get_instance(hosts, profile)
    try:
        if not isinstance(index, list):
            index=[index]
        if es.indices.exists(index=index):
            return True
    except elasticsearch.exceptions.NotFoundError:
        return False
    return False


def index_get(index, hosts=None, profile='elasticsearch'):
    '''
    Check for the existence of an Elasticsearch index and if it's existent, return index details

    CLI example::

        salt myminion elasticsearch.index_get testindex
    '''
    es = _get_instance(hosts, profile)

    try:
        if index_exists(index):
            ret = es.indices.get(index=index) # TODO error handling
            return ret
    except elasticsearch.exceptions.NotFoundError:
        return None
    return None


def mapping_create(index, doc_type, body, hosts=None, profile='elasticsearch'):
    '''
    Create a mapping in a given index

    CLI example::

        salt myminion elasticsearch.mapping_create testindex user '{ "user" : { "properties" : { "message" : {"type" : "string", "store" : true } } } }'
    '''
    es = _get_instance(hosts, profile)
    try:
        result = es.indices.put_mapping(index=index, doc_type=doc_type, body=body) # TODO error handling
        return mapping_get(index, doc_type)
    except elasticsearch.exceptions.NotFoundError:
        return False
    return False


def mapping_delete(index, doc_type, hosts=None, profile='elasticsearch'):
    '''
    Delete a mapping (type) along with its data

    CLI example::

        salt myminion elasticsearch.mapping_delete testindex user
    '''
    es = _get_instance(hosts, profile)
    try:
        # TODO check if mapping exists, add method mapping_exists()
        result = es.indices.delete_mapping(index=index, doc_type=doc_type)

        if result.get('acknowledged', False): # TODO error handling
            return True
    except elasticsearch.exceptions.NotFoundError:
        return False
    return False


def mapping_get(index, doc_type, hosts=None, profile='elasticsearch'):
    '''
    Retrieve mapping definition of index or index/type

    CLI example::

        salt myminion elasticsearch.mapping_get testindex user
    '''
    es = _get_instance(hosts, profile)

    try:
        ret = es.indices.get_mapping(index=index, doc_type=doc_type) # TODO error handling
        return ret
    except elasticsearch.exceptions.NotFoundError:
        return None
    return None


def index_template_create(index, doc_type, body, hosts=None, profile='elasticsearch'):
    '''
    Create an index template

    CLI example::

        salt myminion elasticsearch.template_create testindex user '{ "user" : { "properties" : { "message" : {"type" : "string", "store" : true } } } }'
    '''
    es = _get_instance(hosts, profile)
    try:
        result = es.indices.put_template(index=index, doc_type=doc_type, body=body) # TODO error handling
        return template_get(index, doc_type)
    except elasticsearch.exceptions.NotFoundError:
        return False
    return False


def template_delete(index, doc_type, hosts=None, profile='elasticsearch'):
    '''
    Delete a template (type) along with its data

    CLI example::

        salt myminion elasticsearch.template_delete testindex user
    '''
    es = _get_instance(hosts, profile)
    try:
        # TODO check if template exists, add method template_exists()
        result = es.indices.delete_template(index=index, doc_type=doc_type)

        if result.get('acknowledged', False): # TODO error handling
            return True
    except elasticsearch.exceptions.NotFoundError:
        return False
    return False


def template_get(index, doc_type, hosts=None, profile='elasticsearch'):
    '''
    Retrieve template definition of index or index/type

    CLI example::

        salt myminion elasticsearch.template_get testindex user
    '''
    es = _get_instance(hosts, profile)

    try:
        ret = es.indices.get_template(index=index, doc_type=doc_type) # TODO error handling
        return ret
    except elasticsearch.exceptions.NotFoundError:
        return None
    return None


####################################
def exists(index, id, doc_type='_all', hosts=None, profile='elasticsearch'):
    '''
    Check for the existence of an elasticsearch document specified by id in the
    index.

    CLI example::

        salt myminion elasticsearch.exists testindex mydash profile='grafana'
    '''
    es = _get_instance(hosts, profile)
    try:
        return es.exists(index=index, id=id, doc_type=doc_type)
    except elasticsearch.exceptions.NotFoundError:
        return False


def index_foo(index, doc_type, body, id=None, hosts=None, profile='elasticsearch'):
    '''
    Create or update an index with the specified body for the specified id.

    CLI example::

        salt myminion elasticsearch.index testindex dashboard '{"user":"guest","group":"guest","body":"",...}' mydash profile='grafana'
    '''
    es = _get_instance(hosts, profile)
    return es.index(index=index, doc_type=doc_type, body=body, id=id)


def get(index, id, doc_type='_all', hosts=None, profile='elasticsearch'):
    '''
    Get the contents of the specifed id from the index.

    CLI example::

        salt myminion elasticsearch.get testindex mydash profile='grafana'
    '''
    es = _get_instance(hosts, profile)
    return es.get(index=index, id=id, doc_type=doc_type)


def delete(index, doc_type, id, hosts=None, profile='elasticsearch'):
    '''
    Delete the document specified by the id in the index.

    CLI example::

        salt myminion elasticsearch.delete testindex dashboard mydash profile='grafana'
    '''
    es = _get_instance(hosts, profile)
    try:
        es.delete(index=index, doc_type=doc_type, id=id)
        return True
    except elasticsearch.exceptions.NotFoundError:
        return True
    except Exception:
        return False


def _get_instance(hosts, profile):
    '''
    Return the elasticsearch instance
    '''
    if profile:
        if isinstance(profile, string_types):
            _profile = __salt__['config.option'](profile)
        elif isinstance(profile, dict):
            _profile = profile
        if _profile:
            hosts = _profile.get('host')
            if not hosts:
                hosts = _profile.get('hosts')
    if isinstance(hosts, string_types):
        hosts = [hosts]
    return elasticsearch.Elasticsearch(hosts)
