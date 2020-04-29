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

import occo.util.config as cfg
import occo.enactor as enactor
import occo.compiler as compiler
import occo.infobroker as ib
import occo.util.communication as comm
import occo.util.factory as factory
import occo.util as util
import occo.constants.status as nodestate
import occo.util.config as config
from occo.infobroker.uds import UDS
import occo.infobroker.rediskvstore
from functools import wraps
import uuid, sys
import io as sio
import unittest
import nose
import logging
import logging.config

CFG_FILE=util.rel_to_file('test_configuration.yaml')
TEST_CFG_FILE=util.rel_to_file('test_input.yaml')
infracfg = config.DefaultYAMLConfig(TEST_CFG_FILE)
infracfg.parse_args([])
cfg = config.DefaultYAMLConfig(CFG_FILE)
cfg.parse_args([])

logging.config.dictConfig(cfg.logging)

log = logging.getLogger()

class SingletonLocalInstruction(object):
    def __init__(self, parent_ip, **kwargs):
        self.parent_ip = parent_ip
        self.__dict__.update(kwargs)
    def perform(self):
        raise NotImplementedError()
class CreateInfrastructureSLI(SingletonLocalInstruction):
    def __init__(self, parent_ip, infra_id, **kwargs):
        self.infra_id = infra_id
        super(CreateInfrastructureSLI, self).__init__(parent_ip, **kwargs)
    def perform(self):
        log.debug('Perform CreateInfrastructure %r', self.infra_id)
        self.parent_ip.started = True
    def __str__(self):
        return '{{create_infrastructure -> {0}}}'.format(self.infra_id)
class CreateNodeSLI(SingletonLocalInstruction):
    def __init__(self, parent_ip, node_def, **kwargs):
        self.node_def = node_def
        super(CreateNodeSLI, self).__init__(parent_ip, **kwargs)
    def start_process(self):
        # Process execution/fork can be done here
        return str(uuid.uuid4())
    def perform(self):
        pid = self.start_process()
        log.debug('Perform CreateNode %r -> %r', self.node_def, pid)
        self.parent_ip.add_process(self.node_def['name'], pid)
    def __str__(self):
        return '{{create_node -> {0}}}'.format(self.node_def['name'])
class DropNodeSLI(SingletonLocalInstruction):
    def __init__(self, parent_ip, instance_data, **kwargs):
        self.node_id = instance_data['node_id']
        super(DropNodeSLI, self).__init__(parent_ip, **kwargs)
    def perform(self):
        log.debug('Perform DropNode %r', self.node_id)
        self.parent_ip.drop_process(self.node_id)
    def __str__(self):
        return '{{drop_node -> {0}}}'.format(self.node_id)
class DropInfrastructureSLI(SingletonLocalInstruction):
    def __init__(self, parent_ip, infra_id, **kwargs):
        self.infra_id = infra_id
        super(DropInfrastructureSLI, self).__init__(parent_ip, **kwargs)
    def perform(self):
        log.debug('Perform DropInfrastructure %r', self.infra_id)
        self.parent_ip.started = False
    def __str__(self):
        return '{{drop_infrastructure -> {0}}}'.format(self.infra_id)

@factory.register(comm.RPCProducer, 'local')
@ib.provider
class SingletonLocalInfraProcessor(ib.InfoProvider,
                                   comm.RPCProducer):
    def __init__(self, static_description, uds, **kwargs):
        ib.InfoProvider.__init__(self, main_info_broker=True)
        self.static_description = static_description
        self.process_list = \
            dict((n, []) for n in static_description.node_lookup.keys())
        self.process_lookup = dict()
        self.started = False
        self.uds = uds
        self.uds.add_infrastructure(static_description)

    def add_process(self, node_name, pid):
        self.process_list[node_name].append(pid)
        self.process_lookup[pid] = node_name
        self.uds.register_started_node(
            self.static_description.infra_id,
            node_name,
            dict(node_id=pid,
                 name=node_name,
                 state='ready'))

    def drop_process(self, pid):
        node_name = self.process_lookup.pop(pid)
        self.process_list[node_name].remove(pid)

    @ib.provides('infrastructure.started')
    def infrastructure_created(self, infra_id, **kwargs):
        return self.started

    @ib.provides('infrastructure.name')
    def infra_name(self, infra_id, **kwargs):
        return self.static_description.name

    @ib.provides('infrastructure.static_description')
    def infra_descr(self, infra_id, **kwargs):
        return self.static_description

    @ib.provides('infrastructure.state')
    def infra_state(self, infra_id, allow_default=False):
        return self.uds.get_infrastructure_state(infra_id, allow_default)

    def cri_create_infrastructure(self, infra_id):
        return CreateInfrastructureSLI(
            self, instruction='create_infrastructure', infra_id=infra_id)
    def cri_create_node(self, node):
        return CreateNodeSLI(self, instruction='create_node', node_def=node)
    def cri_drop_node(self, instance_data):
        return DropNodeSLI(self, instruction='drop_node',
                           instance_data=instance_data)
    def cri_drop_infrastructure(self, infra_id):
        return DropInfrastructureSLI(
            self, instruction='drop_infrastructure', infra_id=infra_id)
    def push_instructions(self, instructions, **kwargs):
        for i in instructions:
            i.perform()

@factory.register(comm.RPCProducer, 'local_test')
class SLITester(SingletonLocalInfraProcessor):
    def __init__(self, statd, uds, output_buffer, **kwargs):
        super(SLITester, self).__init__(statd, uds, **kwargs)
        self.buf = output_buffer
        self.print_state()
    def print_state(self):
        self.buf.write('R' if self.started else 'S')
        state = self.process_list
        for k in sorted(state.keys()):
            self.buf.write(' {0}:{1}'.format(k, len(state[k])))
        self.buf.write('\n')
    def push_instructions(self, instructions, **kwargs):
        super(SLITester, self).push_instructions(instructions, **kwargs)
        self.print_state()

def make_enactor_pass(infra, uds,
                      upkeep_strategy='noop',
                      downscale_strategy='simple'):
    buf = sio.StringIO()
    statd = compiler.StaticDescription(infra)
    processor = comm.RPCProducer.instantiate('local_test', statd, uds, buf)
    e = enactor.Enactor(infrastructure_id=statd.infra_id,
                        infraprocessor=processor,
                        upkeep_strategy=upkeep_strategy,
                        downscale_strategy=downscale_strategy)
    e.make_a_pass()
    nose.tools.assert_equal(buf.getvalue(),
                            infra['expected_output'])
    return e, buf, statd

def test_enactor_pass():
    uds = UDS.instantiate(protocol='dict')
    for infra in infracfg.infrastructures:
        yield make_enactor_pass, infra, uds

def make_upkeep(uds_config):
    import copy
    infra = copy.deepcopy(infracfg.infrastructures[0])
    uds = UDS.instantiate(**uds_config)
    e, buf, statd = make_enactor_pass(
        infra,
        uds,
        upkeep_strategy='basic')
    nose.tools.assert_equal(buf.getvalue(),
                            infra['expected_output'])

    statekey = 'infra:{0}:state'.format(statd.infra_id)
    failedkey = 'infra:{0}:failed_nodes'.format(statd.infra_id)
    dynstate = uds.kvstore[statekey]
    origstate = copy.deepcopy(dynstate)

    list(dynstate['C'].values())[1]['state'] = nodestate.SHUTDOWN
    list(dynstate['A'].values())[0]['state'] = nodestate.FAIL
    uds.kvstore[statekey] = dynstate
    e.make_a_pass()

    dynstate = uds.kvstore[statekey]
    nose.tools.assert_equal((len(dynstate['A']), len(dynstate['C'])),
                            (len(origstate['A']), len(origstate['C'])))

    nose.tools.assert_equal(
        list(uds.kvstore[failedkey].values())[0]['node_id'],
        list(origstate['A'].values())[0]['node_id'])

def test_upkeep():
    yield make_upkeep, dict(protocol='dict')
    yield make_upkeep, dict(protocol='redis')

def test_drop_nodes():
    import copy
    infra = copy.deepcopy(infracfg.infrastructures[0])
    uds = UDS.instantiate(protocol='dict')
    e, buf, statd = make_enactor_pass(infra, uds)
    nose.tools.assert_equal(buf.getvalue(),
                            infra['expected_output'])
    sc = infra['nodes'][2]['scaling']
    sc['min'] = sc['max'] = 1
    e.make_a_pass()

def setup_module():
    import os
    log.info('PID: %d', os.getpid())
