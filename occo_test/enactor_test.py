#
# Copyright (C) 2014 MTA SZTAKI
#

import occo.util.config as cfg
import occo.enactor as enactor
import occo.compiler as compiler
import occo.infobroker as ib
import occo.util.communication as comm
import occo.util.factory as factory
import occo.util as util
import occo.util.config as config
from occo.infobroker.uds import UDS
from functools import wraps
import uuid, sys
import StringIO as sio
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
        self.parent_ip.started = True
    def __str__(self):
        return '{create_infrastructure -> %s}'%self.infra_id
class CreateNodeSLI(SingletonLocalInstruction):
    def __init__(self, parent_ip, node_def, **kwargs):
        self.node_def = node_def
        super(CreateNodeSLI, self).__init__(parent_ip, **kwargs)
    def start_process(self):
        # Process execution/fork can be done here
        return str(uuid.uuid4())
    def perform(self):
        pid = self.start_process()
        self.parent_ip.add_process(self.node_def['name'], pid)
    def __str__(self):
        return '{create_node -> %s}'%self.node_def['name']
class DropNodeSLI(SingletonLocalInstruction):
    def __init__(self, parent_ip, instance_data, **kwargs):
        self.node_id = instance_data['node_id']
        super(DropNodeSLI, self).__init__(parent_ip, **kwargs)
    def perform(self):
        self.parent_ip.drop_process(self.node_id)
    def __str__(self):
        return '{drop_node -> %s}'%self.node_id
class DropInfrastructureSLI(SingletonLocalInstruction):
    def __init__(self, parent_ip, infra_id, **kwargs):
        self.infra_id = infra_id
        super(DropInfrastructureSLI, self).__init__(parent_ip, **kwargs)
    def perform(self):
        self.parent_ip.started = False
    def __str__(self):
        return '{drop_infrastructure -> %s}'%self.infra_id

@factory.register(comm.RPCProducer, 'local')
@ib.provider
class SingletonLocalInfraProcessor(ib.InfoProvider,
                                   comm.RPCProducer):
    def __init__(self, static_description, uds, **kwargs):
        ib.InfoProvider.__init__(self, main_info_broker=True)
        self.static_description = static_description
        self.process_list = \
            dict((n, []) for n in static_description.node_lookup.iterkeys())
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
    def infra_state(self, infra_id, **kwargs):
        return self.uds.get_infrastructure_state(infra_id)

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
        for k in sorted(state.iterkeys()):
            self.buf.write(' %s:%d'%(k, len(state[k])))
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
