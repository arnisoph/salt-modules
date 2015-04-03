# -*- coding: utf-8 -*-
'''
State module to manage Elasticsearch indices

'''

# Import python libs
from __future__ import absolute_import
import logging

# Import salt libs

log = logging.getLogger(__name__)

# Import third party libs
try:
    import elasticsearch  #TODO use test function of custom modules?
    HAS_ELASTICSEARCH = True
except ImportError:
    HAS_ELASTICSEARCH = False


def __virtual__():
    '''
    Only load if elasticsearch libraries exist.
    '''
    if not HAS_ELASTICSEARCH:
        return False
    return True


def absent(name):
    '''
    Ensure that the named index is absent
    '''

    ret = {'name': name, 'changes': {}, 'result': True, 'comment': ''}

    index_exists = __salt__['elasticsearcharbe.index_exists'](index=name)
    if index_exists:
        if __opts__['test']:
            ret['comment'] = 'Index {0} will be removed'.format(name)
            ret['result'] = None
        else:
            ret['result'] = __salt__['elasticsearcharbe.index_delete'](index=name)

            if ret['result']:
                ret['comment'] = 'Removed index {0} successfully'.format(name)
            else:
                ret['comment'] = 'Failed to remove index {0}'.format(name)  #TODO error handling
    elif index_exists == False:
        ret['comment'] = 'Index {0} is already absent'.format(name)
    else:
        ret['comment'] = 'Failed to determine whether index {0} is absent, see Minion log for more information'.format(
            name)
        ret['result'] = False

    return ret


def present(name, body={}):
    '''
    Ensure that the named index is present
    '''

    ret = {'name': name, 'changes': {}, 'result': True, 'comment': ''}

    index_exists = __salt__['elasticsearcharbe.index_exists'](name)
    if index_exists == False:
        if __opts__['test']:
            ret['comment'] = 'Index {0} will be created'.format(name)
            ret['result'] = None
        else:
            ret['result'] = __salt__['elasticsearcharbe.index_create'](index=name, body=body)
            # TODO show pending changes (body)?
            ret['changes'] = __salt__['elasticsearcharbe.index_get'](index=name)

            if ret['result']:
                ret['comment'] = 'Created index {0} successfully'.format(name)
    elif index_exists:
        ret['comment'] = 'Index {0} is already present'.format(name)
    else:
        ret['comment'] = 'Failed to determine whether index {0} is present, see Minion log for more information'.format(
            name)
        ret['result'] = False

    return ret
