from flask import request
from flask_restful import Resource

import json
import traceback

from common import *
from functools import wraps

## We need this to access the instance of the module
#
# We can't use 'from module import instance' because
# the instance is not ready, yet (would be None)
import module


# Helper function to catch and log the exceptions
def catch(f):
    @wraps(f)
    def catcher(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except:
            module.instance.log.error(str(traceback.format_exc()))
            return {'error': str(traceback.format_exc()).split('\n')}
    return catcher



class Index(Resource):
    _neverest_endpoint = '/'

    @catch
    def get(self):
        return {
            'api_version': 1,
            'info': "Ceph Manager RESTful API server"
        }



class ConfigCluster(Resource):
    _neverest_endpoint = '/config/cluster'

    @catch
    def get(self):
        return module.instance.get("config")



class ConfigClusterKey(Resource):
    _neverest_endpoint = '/config/cluster/<string:key>'

    @catch
    def get(self, key):
        return module.instance.get("config").get(key, None)



class ConfigOsd(Resource):
    _neverest_endpoint = '/config/osd'

    @catch
    def get(self):
        return module.instance.get("osd_map")['flags'].split(',')


    @catch
    def patch(self):
        args = json.loads(request.data)

        commands = []

        valid_flags = set(args.keys()) & set(OSD_FLAGS)
        invalid_flags = list(set(args.keys()) - valid_flags)
        if invalid_flags:
            module.instance.log.warn("%s not valid to set/unset" % invalid_flags)

        for flag in list(valid_flags):
            if args[flag]:
                mode = 'set'
            else:
                mode = 'unset'

            commands.append({
                'prefix': 'osd ' + mode,
                'key': flag,
            })

        return module.instance.submit_request([commands])



class Server(Resource):
    _neverest_endpoint = '/server'

    @catch
    def get(self):
        return module.instance.list_servers()



class ServerFqdn(Resource):
    _neverest_endpoint = '/server/<string:fqdn>'

    @catch
    def get(self, fqdn):
        return module.instance.get_server(fqdn)



class Mon(Resource):
    _neverest_endpoint = '/mon'

    @catch
    def get(self):
        return module.instance.get_mons()



class MonName(Resource):
    _neverest_endpoint = '/mon/<string:name>'

    @catch
    def get(self, name):
        mons = filter(lambda x: x['name'] == name, module.instance.get_mons())
        if len(mons) != 1:
                return None
        return mons[0]



class Osd(Resource):
    _neverest_endpoint = '/osd'

    @catch
    def get(self):
        # Parse request args
        ids = request.args.getlist('id[]')
        pool_id = request.args.get('pool', None)

        return module.instance.get_osds(ids, pool_id)



class OsdId(Resource):
    _neverest_endpoint = '/osd/<int:osd_id>'

    @catch
    def get(self, osd_id):
        osd = module.instance.get_osds([str(osd_id)])
        if len(osd) != 1:
            return {'error': 'Failed to identify the OSD.'}

        return osd[0]


    @catch
    def patch(self, osd_id):
        args = json.loads(request.data)

        commands = []

        osd_map = module.instance.get('osd_map')

        if 'in' in args:
            if args['in']:
                commands.append({
                    'prefix': 'osd in',
                    'ids': [str(osd_id)]
                })
            else:
                commands.append({
                    'prefix': 'osd out',
                    'ids': [str(osd_id)]
                })

        if 'up' in args:
            if args['up']:
                return {'error': "It is not valid to set a down OSD to be up"}
            else:
                commands.append({
                    'prefix': 'osd down',
                    'ids': [str(osd_id)]
                })

        if 'reweight' in args:
            commands.append({
                'prefix': 'osd reweight',
                'id': osd_id,
                'weight': args['reweight']
            })

        return module.instance.submit_request([commands])



class OsdIdCommand(Resource):
    _neverest_endpoint = '/osd/<int:osd_id>/command'

    @catch
    def get(self, osd_id):
        osd = module.instance.get_osd_by_id(osd_id)

        if not osd:
            return {'error': 'Failed to identify the OSD'}

        if osd['up']:
            return OSD_IMPLEMENTED_COMMANDS
        else:
            return []



class OsdIdCommandId(Resource):
    _neverest_endpoint = '/osd/<int:osd_id>/command/<string:command>'

    @catch
    def post(self, osd_id, command):
        osd = module.instance.get_osd_by_id(osd_id)

        if not osd:
            return {'error': 'Failed to identify the OSD'}

        if not osd['up'] or command not in OSD_IMPLEMENTED_COMMANDS:
            return {'error': 'Command "%s" not available' % command}

        return module.instance.submit_request([[{
            'prefix': 'osd ' + command,
            'who': str(osd_id)
        }]])



class Pool(Resource):
    _neverest_endpoint = '/pool'

    @catch
    def get(self):
        return module.instance.get('osd_map')['pools']


    @catch
    def post(self):
        args = json.loads(request.data)

        for arg in ['name', 'pg_num']:
            if arg not in args:
                return {'error': 'Argument "%s" is missing' % arg}

        # Run the pool create command first
        create_command = {
            'prefix': 'osd pool create',
            'pool': args['name'],
            'pg_num': args['pg_num']
        }

        # Remove pg_num so that we won't try to change it later, again
        del args['pg_num']

        set_commands = []
        # Run the pool setting properties in parallel
        for var in POOL_PROPERTIES:
            if var in args:
                set_commands.append({
                    'prefix': 'osd pool set',
                    'pool': args['name'],
                    'var': var,
                    'val': args[var],
                })
                del args[var]

        for (var, field) in POOL_QUOTA_PROPERTIES:
            if var in args:
                set_commands.append({
                    'prefix': 'osd pool set-quota',
                    'pool': args['name'],
                    'field': field,
                    'val': str(args[var]),
                })
                del args[var]

        # Check that there are no other args
        del args['name']
        if len(args) != 0:
            return {'error': 'Invalid arguments found: "%s"' % str(args)}

        return module.instance.submit_request([[create_command]] + [set_commands])



class PoolId(Resource):
    _neverest_endpoint = '/pool/<int:pool_id>'

    @catch
    def get(self, pool_id):
        pool = filter(
            lambda x: x['pool'] == pool_id,
            module.instance.get('osd_map')['pools'],
        )

        if len(pool) != 1:
            return {'error': 'Failed to identify the pool'}

        return pool[0]


    @catch
    def patch(self, pool_id):
        pass


    @catch
    def delete(self, pool_id):
        pass



class Request(Resource):
    _neverest_endpoint = '/request'

    @catch
    def get(self):
        states = {}
        for request in module.instance.requests:
            states[request.uuid] = request.get_state()

        return states


    @catch
    def delete(self):
        num_requests = len(module.instance.requests)

        module.instance.requests = filter(
            lambda x: not x.is_finished(),
            module.instance.requests
        )

        # Return the number of jobs cleaned
        return num_requests - len(module.instance.requests)



class RequestUuid(Resource):
    _neverest_endpoint = '/request/<string:uuid>'

    @catch
    def get(self, uuid):
        request = filter(
            lambda x: x.uuid == uuid,
            module.instance.requests
        )

        if len(request) != 1:
            return {'error': 'Unknown request UUID "%s"' % str(uuid)}

        request = request[0]
        return {
            'uuid': request.uuid,
            'running': map(
                lambda x: x.command,
                request.running
            ),
            'finished': map(
                lambda x: x.command,
                request.finished
            ),
            'waiting': map(
                lambda x: x.command,
                request.waiting
            ),
            'failed': map(
                lambda x: x.command,
                request.failed
            ),
            'is_waiting': request.is_waiting(),
            'is_finished': request.is_finished(),
            'has_failed': request.has_failed(),
        }


    @catch
    def delete(self, uuid):
        for index in range(len(module.instance.requests)):
            if module.instance.requests[index].uuid == uuid:
                module.instance.requests.pop(index)
                return True

        # Failed to find the job to cancel
        return False
