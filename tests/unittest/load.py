#
# Copyright (C) 2014 MTA SZTAKI
#

import occo.util.config as cfg
import occo.enactor as enactor
import occo.compiler as compiler
import occo.infobroker as ib
import occo.util.communication as comm
from functools import wraps
import uuid, sys

with open('test-config.yaml') as f:
    config = cfg.DefaultYAMLConfig(f)
    config.parse_args()

class SingletonLocalInstruction(object):
    def __init__(self, parent_ip, **kwargs):
        self.parent_ip = parent_ip
        self.__dict__.update(kwargs)
    def perform(self):
        raise NotImplementedError()
class CreateEnvironmentSLI(SingletonLocalInstruction):
    def __init__(self, parent_ip, enviro_id, **kwargs):
        self.enviro_id = enviro_id
        super(CreateEnvironmentSLI, self).__init__(parent_ip, **kwargs)
    def perform(self):
        self.parent_ip.started = True
    def __str__(self):
        return '{create_environment -> %s}'%self.enviro_id
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
    def __init__(self, parent_ip, node_id, **kwargs):
        self.node_id = node_id
        super(DropNodeSLI, self).__init__(parent_ip, **kwargs)
    def perform(self):
        self.parent_ip.drop_process(self.node_id)
    def __str__(self):
        return '{drop_node -> %s}'%self.node_id
class DropEnvironmentSLI(SingletonLocalInstruction):
    def __init__(self, parent_ip, enviro_id, **kwargs):
        self.enviro_id = enviro_id
        super(DropEnvironmentSLI, self).__init__(parent_ip, **kwargs)
    def perform(self):
        self.parent_ip.started = False
    def __str__(self):
        return '{drop_environment -> %s}'%self.enviro_id

@comm.register(comm.RPCProducer, 'local')
@ib.provider
class SingletonLocalInfraProcessor(ib.InfoProvider,
                                   comm.RPCProducer):
    def __init__(self, static_description, **kwargs):
        self.static_description = static_description
        self.process_list = \
            dict((n, []) for n in static_description.node_lookup.iterkeys())
        self.process_lookup = dict()
        self.started = False

    def add_process(self, node_name, pid):
        self.process_list[node_name].append(pid)
        self.process_lookup[pid] = node_name
    def drop_process(self, pid):
        node_name = self.process_lookupi.pop(pid)
        self.process_list[node_name].remove(pid)

    @ib.provides('infrastructure.started')
    def enviro_created(self, infra_id, **kwargs):
        return self.started

    @ib.provides('infrastructure.name')
    def infra_name(self, infra_id, **kwargs):
        return self.static_description.name
    
    @ib.provides('infrastructure.static_description')
    def infra_descr(self, infra_id, **kwargs):
        return self.static_description

    @ib.provides('infrastructure.state')
    def infra_state(self, infra_id, **kwargs):
        return self.process_list

    def cri_create_env(self, environment_id):
        return CreateEnvironmentSLI(
            self, instruction='create_environment', enviro_id=environment_id)
    def cri_create_node(self, node):
        return CreateNodeSLI(self, instruction='create_node', node_def=node)
    def cri_drop_node(self, node_id):
        return DropNodeSLI(self, instruction='drop_node', node_id=node_id)
    def cri_drop_env(self, environment_id):
        return DropEnvironmentSLI(
            self, instruction='drop_environment', enviro_id=environment_id)
    def push_instructions(self, instructions, **kwargs):
        for i in instructions:
            i.perform()

@comm.register(comm.RPCProducer, 'local_test')
class SLITester(SingletonLocalInfraProcessor):
    def __init__(self, statd, output_buffer, **kwargs):
        super(SLITester, self).__init__(statd, **kwargs)
        self.buf = output_buffer
        self.print_state()
    def print_state(self):
        self.buf.write('R' if self.started else 'S')
        for k, v in self.get('infrastructure.state',
                             self.static_description.infra_id).iteritems():
            self.buf.write(' %s:%d'%(k, len(v)))
        self.buf.write('\n')
    def push_instructions(self, instructions, **kwargs):
        super(SLITester, self).push_instructions(instructions, **kwargs)
        self.print_state()

statd = compiler.StaticDescription(config.infrastructure)
processor = comm.RPCProducer(statd, sys.stdout, protocol='local_test')
e = enactor.Enactor(infrastructure_id=statd.infra_id,
            infobroker=processor,
            infraprocessor=processor)
e.make_a_pass()
