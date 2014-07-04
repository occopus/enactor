#
# Copyright (C) 2014 MTA SZTAKI
#

import occo.util.config as cfg
import occo.enactor as enactor
import occo.compiler as compiler
import occo.infobroker as ib
import occo.util.communication as comm
from functools import wraps

with open('test-config.yaml') as f:
    config = cfg.DefaultYAMLConfig(f)
    config.parse_args()

@comm.register(comm.RPCProducer, 'local')
@ib.provider
class SingletonLocalInfraProcessor(ib.InfoProvider,
                                   comm.RPCProducer):
    def __init__(self, static_description, **kwargs):
        self.static_description = static_description
        self.process_list = \
            dict((n, []) for n in static_description.node_lookup.iterkeys())
        self.started = False

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

    def start_process(self, msg):
        pass
    def stop_process(self, msg):
        pass
    def create_environment(self, msg):
        self.started = True
    def drop_environment(self, msg):
        self.started = False

    def push_message(self, message, **kwargs):
        pass

statd = compiler.StaticDescription(config.infrastructure)
processor = SingletonLocalInfraProcessor(statd, protocol='local')
e = enactor.Enactor(infrastructure_id=statd.infra_id,
            infobroker=processor,
            infraprocessor=processor)
e.make_a_pass()
print processor.get('infrastructure.state', infra_id=statd.infra_id)