import json


def contains(keys, dictionary):
    for key in keys:
        if key not in dictionary:
            return False
    return True


def missing(keys, dictionary):
    return [key for key in keys if key not in dictionary]


def from_request(request):
    data = request.get_json() or request.data.decode('utf-8')
    if data is None or len(data) == 0:
        error_msg = json.dumps({'success': False,
                                'error': 'Empty config',
                                'info': request})
        raise Exception(error_msg)

    try:
        config = json.loads(data)
    except json.JSONDecodeError:
        error_msg = json.dumps({'success': False,
                                'error': 'Json decode error',
                                'info': data})
        raise Exception(error_msg)

    if not contains(['workspaces', 'prices'], config):
        missing_keys = '\n- '.join(missing(['workspaces', 'prices'], config))
        error_msg = json.dumps({'success': False,
                                'error': f'Missing config info: '
                                         f'{missing_keys}',
                                'info': config})
        raise Exception(error_msg)

    req_workspace_keys = ['url', 'id', 'type', 'name', 'token']
    for workspace in config['workspaces']:
        if not contains(req_workspace_keys, workspace):
            missing_keys = '\n- '.join(missing(req_workspace_keys, workspace))
            error_msg = json.dumps({'success': False,
                                    'error': f'Missing workspace info: '
                                             f'{missing_keys}',
                                    'info': workspace})
            raise Exception(error_msg)

    req_prices_keys = ['interactive', 'job']
    if not contains(req_prices_keys, config['prices']):
        missing_keys = '\n- '.join(missing(req_prices_keys, config['prices']))
        error_msg = json.dumps({'success': False,
                                'error': f'Missing config info: '
                                         f'{missing_keys}',
                                'info': config['prices']})
        raise Exception(error_msg)

    return config


def save(config, path):
    try:
        with open(path, 'w') as jsonfile:
            json.dump(config, jsonfile)
    except Exception as e:
        error_msg = json.dumps({'success': False,
                                'error': f'Saving config to {path} failed',
                                'info': str(e)})
        raise Exception(error_msg)

    return {'success': True, 'error': None, 'info': config}


def format_workspace_configs(configs):
    if not isinstance(configs, list):
        configs = [configs]

    formatted = []
    for config in configs:
        key_value = '\n\t'.join([f"'{k}': '{v}'" for k, v in config.items()])
        formatted.append(f'{{\n\t{key_value}\n}}')

    return ',\n'.join(formatted)
