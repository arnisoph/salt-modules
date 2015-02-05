# -*- coding: utf-8 -*-
# vim: ts=4 sw=4 et

# TODO add documentation

from __future__ import absolute_import
import json
import os
import yaml

import salt.fileclient
import salt.utils
from salt.utils.dictupdate import update


def _mk_file_client():
    '''
    Create a file client and add it to the context
    '''
    if 'cp.fileclient' not in __context__:
        __context__['cp.fileclient'] = salt.fileclient.get_file_client(__opts__)


def _cache_files(formula, file_extensions, saltenv):
    '''
    Generates a list of salt://<pillar_name>/defaults.(json|yaml) files
    and fetches them from the Salt master.
    '''
    _mk_file_client()
    formula = formula.replace('.', '/')
    cached_files = {}

    for ext in file_extensions:
        for file_name in ['defaults', 'custom_defaults']:
            source_url = 'salt://{formula}/{file_name}.{file_ext}'.format(formula=formula, file_name=file_name, file_ext=ext)
            cached_file = __context__['cp.fileclient'].cache_file(source_url, saltenv)

            if cached_file:
                if ext not in cached_files.keys():
                    cached_files[ext] = {}
                cached_files[ext][file_name] = _load_data(cached_file)

    return cached_files


def _load_data(cached_file):
    '''
    Given a pillar_name and the template cache location, attempt to load
    the defaults.json from the cache location. If it does not exist, try
    defaults.yaml.
    '''
    file_name, file_type = os.path.splitext(cached_file)

    if file_type == '.yaml':
        loader = yaml
    elif file_type == '.json':
        loader = json
    else:
        # TODO panic
        pass

    with salt.utils.fopen(cached_file) as fhr:
        data = loader.load(fhr)
    return data


def generate_state(state_module, state_function, attrs=[]):
    attrs.insert(0, state_function)
    return {state_module: attrs}


def get_defaults(formula, saltenv, file_extensions=['yaml', 'json'], merge=True):
    '''
    TODO doc
    '''
    defaults = _cache_files(formula, file_extensions, saltenv)

    if not defaults:
        return {}

    # TODO comment this beast
    # TODO optimize
    if merge:
        merged_maps = {}
        for ext, datamap in defaults.items():
            merged_maps[ext] = {}
            for file_name, rawmaps in datamap.items():
                for grain, rawmap in rawmaps.items():
                    if grain not in merged_maps[ext].keys():
                        merged_maps[ext][grain] = {}
                    merged_maps[ext][grain][file_name] = __salt__['grains.filter_by'](rawmap, grain)

            for grain, file_maps in merged_maps[ext].items():
                defaults_map = merged_maps[ext][grain].get('defaults', {})
                custom_defaults_map = merged_maps[ext][grain].get('custom_defaults', {})
                update(defaults_map, custom_defaults_map)
                merged_maps[ext][grain] = defaults_map

            merged_grain_maps = {}
            for grain, grain_map in merged_maps[ext].items():
                if not merged_grain_maps:
                    merged_grain_maps = grain_map
                    continue

                update(merged_grain_maps, grain_map)
            merged_maps[ext] = merged_grain_maps

            pillar_path = '{formula}:lookup'.format(formula=formula)
            update(merged_maps[ext], __salt__['pillar.get'](pillar_path, {}))

        defaults = merged_maps
    return defaults
