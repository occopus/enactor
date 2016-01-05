### Copyright 2014, MTA SZTAKI, www.sztaki.hu
###
### Licensed under the Apache License, Version 2.0 (the "License");
### you may not use this file except in compliance with the License.
### You may obtain a copy of the License at
###
###    http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.

"""
Upkeep algorithms to be used before making an Enactor pass.

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>
"""

import occo.infobroker as ib
import occo.util.factory as factory
import occo.constants.status as nodestate
import logging

log = logging.getLogger('occo.upkeep')
datalog = logging.getLogger('occo.data.upkeep')

class Upkeep(factory.MultiBackend):
    def __init__(self):
        self.infobroker = ib.main_info_broker

    def acquire_dynamic_state(self, infra_id):
        raise NotImplementedError()

@factory.register(Upkeep, 'noop')
class DefaultUpkeep(Upkeep):
    def acquire_dynamic_state(self, infra_id):
        return self.infobroker.get('infrastructure.state', infra_id, True)

@factory.register(Upkeep, 'basic')
class BasicUpkeep(Upkeep):
    def __init__(self):
        super(BasicUpkeep, self).__init__()
        import occo.infobroker
        self.uds = occo.infobroker.main_uds

    def is_failed(self, node):
        return node['state'] == nodestate.FAIL

    def is_shutdown(self, node):
        return node['state'] == nodestate.SHUTDOWN

    def acquire_dynamic_state(self, infra_id):
        log.debug('Acquiring state of %r', infra_id)
        dynamic_state = self.infobroker.get(
            'infrastructure.state', infra_id, True)
        datalog.debug('%r', dynamic_state)

        log.debug('Processing failed nodes in %r', infra_id)
        nodes = [node
                 for instances in dynamic_state.itervalues()
                 for node in instances.itervalues()]
        failed_nodes, remove_nodes = [], []

        for node in nodes:
            failed = self.is_failed(node)
            shutdown = self.is_shutdown(node)
            if failed or shutdown:
                if failed:
                    failed_nodes.append(node)
                remove_nodes.append(node)
                del dynamic_state[node['resolved_node_definition']['name']][node['node_id']]

        log.info('Archiving failed instances of %r: %r',
                 infra_id, [i['node_id'] for i in failed_nodes])
        self.uds.store_failed_nodes(infra_id, *failed_nodes)

        remove_ids = [i['node_id'] for i in remove_nodes]
        log.info('Removing lost instances from %r: %r',
                 infra_id, remove_ids)
        self.uds.remove_nodes(infra_id, *remove_ids)

        return dynamic_state
