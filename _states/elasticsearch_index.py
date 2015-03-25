# -*- coding: utf-8 -*-

'''
State module to manage Elasticsearch indices

:depends: elasticsearch
'''

# Import python libs
from __future__ import absolute_import
import logging

# Import salt libs

log = logging.getLogger(__name__)


def absent(name):
   '''
   Ensure that the named index is absent
   '''

   ret = {'name': name,
          'changes': {},
          'result': True,
          'comment': ''}

    if __salt__['elasticsearcharbe.index_exists'](name):
        pass

    return ret
