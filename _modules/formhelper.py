# -*- coding: utf-8 -*-
# vim: ts=4 sw=4 et
"""
Module that provides helper functions used by formulas
"""
from __future__ import absolute_import

# Import python libs
from collections import OrderedDict
import json
import os
import yaml

import salt.fileclient
import salt.utils
import salt.utils.dictupdate


def __sorted_dict(var):
    """
    Return a sorted dict as OrderedDict
    """
    ret = OrderedDict()
    for key in sorted(list(var.keys())):
        ret[key] = var[key]
    return ret


def _mk_file_client():
    """
    Create a file client and add it to the context
    """
    if 'cp.fileclient' not in __context__:  # NOQA
        __context__['cp.fileclient'] = salt.fileclient.get_file_client(__opts__)  # NOQA


def _cache_files(formula, file_extension, saltenv):
    """
    Generates a list of salt://<pillar_name>/defaults.(json|yaml) files
    and fetches them from the Salt master.
    """
    _mk_file_client()
    formula = formula.replace('.', '/')
    cached_files = {}

    for file_name in ['defaults', 'custom_defaults']:
        source_url = 'salt://{formula}/{file_name}.{file_ext}'.format(formula=formula, file_name=file_name, file_ext=file_extension)
        cached_file = __context__['cp.fileclient'].cache_file(source_url, saltenv)  # NOQA

        if cached_file:
            cached_files[file_name] = _load_data(cached_file)

    return cached_files


def _load_data(cached_file):
    """
    Given a pillar_name and the template cache location, attempt to load
    the defaults.json from the cache location. If it does not exist, try
    defaults.yaml.
    """
    file_name, file_type = os.path.splitext(cached_file)
    loader = None

    if file_type == '.json':
        loader = json
    else:
        loader = yaml

    with salt.utils.fopen(cached_file) as fhr:
        data = loader.load(fhr)
    return data


def generate_state(state_module, state_function, attrs=[]):
    """
    Generate a SaltStack state definion based on given state module name, function and optional attributes.
    """
    attrs.insert(0, state_function)
    return {state_module: attrs}


def defaults(formula, saltenv='base', file_extension='yaml', merge=True):
    """
    Read a formula's defaults files like ``defaults.(yaml|json)`` and ``custom_defaults.(yaml|json)``,
    filter maps based on grains and override the result with pillars (if set).

    CLI Example:

    .. code-block:: bash

        salt-call formhelper.defaults skeleton
    """

    if isinstance(file_extension, list):
        file_extension = file_extension[0]

    # TODO remove yaml/json level, return data directly
    # TODO add param to specifc formula path/location manually
    defaults_files = _cache_files(formula, file_extension, saltenv)

    if not defaults_files:
        return {}

    # TODO comment this beast
    # TODO optimize
    if merge:
        merged_maps = {}

        for file_name, rawmaps in __sorted_dict(defaults_files).items():
            for grain, rawmap in __sorted_dict(rawmaps).items():
                if grain not in merged_maps.keys():
                    merged_maps[grain] = {}
                merged_maps[grain][file_name] = __salt__['grains.filter_by'](rawmap, grain) or {}  # NOQA

        for grain, file_maps in __sorted_dict(merged_maps).items():
            defaults_map = merged_maps[grain].get('defaults', {})
            custom_defaults_map = merged_maps[grain].get('custom_defaults', {})
            salt.utils.dictupdate.update(defaults_map, custom_defaults_map)
            merged_maps[grain] = defaults_map

        merged_grain_maps = {}
        for grain, grain_map in __sorted_dict(merged_maps).items():

            # Do we have something to merge?
            if grain_map is None:
                continue

            if not merged_grain_maps:
                merged_grain_maps = grain_map
                continue

            salt.utils.dictupdate.update(merged_grain_maps, grain_map)
        merged_maps = merged_grain_maps

        pillar_path = '{formula}:lookup'.format(formula=formula)
        salt.utils.dictupdate.update(merged_maps, __salt__['pillar.get'](pillar_path, {}))  # NOQA

        merged_maps.update({file_extension: dict(merged_maps)})
        return merged_maps
    else:
        return defaults_files

# Alias function for backwards-compatiblity
get_defaults = defaults
