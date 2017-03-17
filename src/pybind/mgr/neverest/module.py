"""
A RESTful API for Ceph
"""

# Global instance to share
instance = None

import json
import errno
import threading
import traceback

import api

from uuid import uuid4
from flask import Flask, request
from OpenSSL import SSL
from flask_restful import Api
from multiprocessing import Process

from common import *
from mgr_module import MgrModule, CommandResult



class CommandsRequest(object):
    """
    This class handles parallel as well as sequential execution of
    commands. The class accept a list of iterables that should be
    executed sequentially. Each iterable can contain several commands
    that can be executed in parallel.
    """


    @staticmethod
    def run(commands, uuid = str(uuid4())):
        """
        A static method that will execute the given list of commands in
        parallel and will return the list of command results.
        """
        results = []
        for index in range(len(commands)):
            tag = '%s:%d' % (str(uuid), index)

            # Store the result
            result = CommandResult(tag)
            result.command = commands[index]
            results.append(result)

            # Run the command
            instance.send_command(result, json.dumps(commands[index]),tag)

        return results


    def __init__(self, commands_arrays):
        self.uuid = str(id(self))

        self.running = []
        self.waiting = commands_arrays[1:]
        self.finished = []
        self.failed = []

        self.lock = threading.RLock()
        if not len(commands_arrays):
            # Nothing to run
            return

        # Process first iteration of commands_arrays in parallel
        try:
            results = self.run(commands_arrays[0], self.uuid)
        except:
            instance.log.error(str(traceback.format_exc()))

        self.running.extend(results)


    def next(self):
        with self.lock:
            if not self.waiting:
                # Nothing to run
                return

            # Run a next iteration of commands
            commands = self.waiting[0]
            self.waiting = self.waiting[1:]

            self.running.extend(self.run(commands, self.uuid))


    def finish(self, tag):
        with self.lock:
            for index in range(len(self.running)):
                if self.running[index].tag == tag:
                    if self.running[index].r == 0:
                        self.finished.append(self.running.pop(index))
                    else:
                        self.failed.append(self.running.pop(index))
                    return True

            # No such tag found
            return False


    def is_running(self, tag):
        for result in self.running:
            if result.tag == tag:
                return True
        return False


    def is_ready(self):
        return not self.running and self.waiting


    def is_waiting(self):
        return bool(self.waiting)


    def is_finished(self):
        return not self.running and not self.waiting


    def has_failed(self):
        return bool(self.failed)


    def get_state(self):
        if not self.is_finished():
            return "pending"

        if self.has_failed():
            return "failed"

        return "success"



class Module(MgrModule):
    COMMANDS = [
            {
                "cmd": "enable_auth "
                       "name=val,type=CephChoices,strings=true|false",
                "desc": "Set whether to authenticate API access by key",
                "perm": "rw"
            },
            {
                "cmd": "auth_key_create "
                       "name=key_name,type=CephString",
                "desc": "Create an API key with this name",
                "perm": "rw"
            },
            {
                "cmd": "auth_key_delete "
                       "name=key_name,type=CephString",
                "desc": "Delete an API key with this name",
                "perm": "rw"
            },
            {
                "cmd": "auth_key_list",
                "desc": "List all API keys",
                "perm": "rw"
            },
    ]


    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        global instance
        instance = self

        self.requests = []

        self.keys = {}
        self.enable_auth = True
        self.app = None
        self.api = None


    def notify(self, notify_type, tag):
        try:
            self._notify(notify_type, tag)
        except:
            self.log.error(str(traceback.format_exc()))


    def _notify(self, notify_type, tag):
        if notify_type == "command":
            self.log.warn("Processing request '%s'" % str(tag))
            request = filter(
                lambda x: x.is_running(tag),
                self.requests)
            self.log.warn("Requests '%s'" % str(request))
            if len(request) != 1:
                self.log.warn("Unknown request '%s'" % str(tag))
                return

            request = request[0]
            request.finish(tag)
            self.log.warn("RUNNING Requests '%s'" % str(request.running))
            self.log.warn("WAITING Requests '%s'" % str(request.waiting))
            self.log.warn("FINISHED Requests '%s'" % str(request.finished))
            if request.is_ready():
                request.next()
        else:
            self.log.debug("Unhandled notification type '%s'" % notify_type)
    #    elif notify_type in ['osd_map', 'mon_map', 'pg_summary']:
    #        self.requests.on_map(notify_type, self.get(notify_type))


    def serve(self):
        try:
            self._serve()
        except:
            self.log.error(str(traceback.format_exc()))


    def _serve(self):
        #self.keys = self._load_keys()
        self.enable_auth = self.get_config_json("enable_auth")
        if self.enable_auth is None:
            self.enable_auth = True

        self.app = Flask('ceph-mgr')
        self.app.config['RESTFUL_JSON'] = {
            'sort_keys': True,
            'indent': 4,
            'separators': (',', ': '),
        }
        self.api = Api(self.app)

        # Add the resources as defined in api module
        for _obj in dir(api):
            obj = getattr(api, _obj)
            # We need this try statement because some objects (request)
            # throw exception on any out of context object access
            try:
                _endpoint = getattr(obj, '_neverest_endpoint', None)
            except:
                _endpoint = None
            if _endpoint:
                self.api.add_resource(obj, _endpoint)

        # use SSL context for https
        context = SSL.Context(SSL.SSLv23_METHOD)
        context.use_privatekey_file('/etc/ssl/private/ceph-mgr-neverest.key')
        context.use_certificate_file('/etc/ssl/certs/ceph-mgr-neverest.crt')

        self.log.warn('RUNNING THE SERVER')
        self.app.run(host = '0.0.0.0', port = 8002, ssl_context = context)
        self.log.warn('FINISHED RUNNING THE SERVER')


    def get_mons(self):
        mon_map_mons = self.get('mon_map')['mons']
        mon_status = json.loads(self.get('mon_status')['json'])

        # Add more information
        for mon in mon_map_mons:
            mon['in_quorum'] = mon['rank'] in mon_status['quorum']
            mon['server'] = self.get_metadata("mon", mon['name'])['hostname']
            mon['leader'] = mon['rank'] == mon_status['quorum'][0]

        return mon_map_mons


    def _get_crush_rule_osds(self, rule):
        nodes_by_id = dict((n['id'], n) for n in self.get('osd_map_tree')['nodes'])

        def _gather_leaf_ids(node):
            if node['id'] >= 0:
                return set([node['id']])

            result = set()
            for child_id in node['children']:
                if child_id >= 0:
                    result.add(child_id)
                else:
                    result |= _gather_leaf_ids(nodes_by_id[child_id])

            return result

        def _gather_descendent_ids(node, typ):
            result = set()
            for child_id in node['children']:
                child_node = nodes_by_id[child_id]
                if child_node['type'] == typ:
                    result.add(child_node['id'])
                elif 'children' in child_node:
                    result |= _gather_descendent_ids(child_node, typ)

            return result

        def _gather_osds(root, steps):
            if root['id'] >= 0:
                return set([root['id']])

            osds = set()
            step = steps[0]
            if step['op'] == 'choose_firstn':
                # Choose all descendents of the current node of type 'type'
                d = _gather_descendent_ids(root, step['type'])
                for desc_node in [nodes_by_id[i] for i in d]:
                    osds |= _gather_osds(desc_node, steps[1:])
            elif step['op'] == 'chooseleaf_firstn':
                # Choose all descendents of the current node of type 'type',
                # and select all leaves beneath those
                for desc_node in [nodes_by_id[i] for i in _gather_descendent_ids(root, step['type'])]:
                    # Short circuit another iteration to find the emit
                    # and assume anything we've done a chooseleaf on
                    # is going to be part of the selected set of osds
                    osds |= _gather_leaf_ids(desc_node)
            elif step['op'] == 'emit':
                if root['id'] >= 0:
                    osds |= root['id']

            return osds

        osds = set()
        for i, step in enumerate(rule['steps']):
            if step['op'] == 'take':
                osds |= _gather_osds(nodes_by_id[step['item']], rule['steps'][i + 1:])
        return osds


    def get_osd_pools(self):
        osds = dict(map(lambda x: (x['osd'], []), self.get('osd_map')['osds']))
        pools = dict(map(lambda x: (x['pool'], x), self.get('osd_map')['pools']))
        crush = osd_map_crush = self.get('osd_map_crush')

        osds_by_pool = {}
        for pool_id, pool in pools.items():
            pool_osds = None
            for rule in [r for r in osd_map_crush['rules'] if r['ruleset'] == pool['crush_ruleset']]:
                if rule['min_size'] <= pool['size'] <= rule['max_size']:
                    pool_osds = self._get_crush_rule_osds(rule)

            osds_by_pool[pool_id] = pool_osds

        for pool_id in pools.keys():
            for in_pool_id in osds_by_pool[pool_id]:
                osds[in_pool_id].append(pool_id)

        return osds


    def get_osds(self, ids = [], pool_id = None):
        # Get data
        osd_map = self.get('osd_map')
        osd_metadata = self.get('osd_metadata')
        osd_map_crush = self.get('osd_map_crush')

        # Update the data with the additional info from the osd map
        osds = osd_map['osds']

        # Filter by osd ids
        if ids:
            osds = filter(
                lambda x: str(x['osd']) in ids,
                osds
            )

        # Get list of pools per osd node
        pools_map = self.get_osd_pools()

        # map osd IDs to reweight
        reweight_map = dict([
            (x.get('id'), x.get('reweight', None))
            for x in self.get('osd_map_tree')['nodes']
        ])

        # Build OSD data objects
        for osd in osds:
            osd['pools'] = pools_map[osd['osd']]
            osd['server'] = osd_metadata.get(str(osd['osd']), {}).get('hostname', None)

            osd['reweight'] = reweight_map.get(osd['osd'], 0.0)

            if osd['up']:
                osd['valid_commands'] = OSD_IMPLEMENTED_COMMANDS
            else:
                osd['valid_commands'] = []

        # Filter by pool
        if pool_id:
            pool_id = int(pool_id)
            osds = filter(
                lambda x: pool_id in x['pools'],
                osds
            )

        return osds


    def get_osd_by_id(self, osd_id):
        osds = self.get('osd_map')['osds']

        osd = filter(
            lambda x: x['osd'] == osd_id,
            osds
        )

        if len(osd) != 1:
            return None

        return osd[0]


    def submit_request(self, request):
        self.requests.append(CommandsRequest(request))
        return self.requests[-1].uuid


    def handle_command(self, cmd):
        self.log.info("handle_command: {0}".format(json.dumps(cmd, indent=2)))
        prefix = cmd['prefix']
        if prefix == "enable_auth":
            enable = cmd['val'] == "true"
            self.set_config_json("enable_auth", enable)
            self.enable_auth = enable
            return 0, "", ""
        elif prefix == "auth_key_create":
            if cmd['key_name'] in self.keys:
                return 0, self.keys[cmd['key_name']], ""
            else:
                self.keys[cmd['key_name']] = self._generate_key()
                self._save_keys()

            return 0, self.keys[cmd['key_name']], ""
        elif prefix == "auth_key_delete":
            if cmd['key_name'] in self.keys:
                del self.keys[cmd['key_name']]
                self._save_keys()

            return 0, "", ""
        elif prefix == "auth_key_list":
            return 0, json.dumps(self._load_keys(), indent=2), ""
        else:
            return -errno.EINVAL, "", "Command not found '{0}'".format(prefix)
