#
# Copyright (C) 2014 MTA SZTAKI
#

import occo.util.config as cfg
import occo.enactor as enactor
import occo.compiler as compiler
import occo.infobroker as ib
from functools import wraps

with open('test-config.yaml') as f:
    config = cfg.DefaultYAMLConfig(f)
    config.parse_args()

@ib.provider
class SingletonLocalInfraProcessor(ib.InfoProvider):
    def __init__(self, static_description):
        self.static_description = static_description
        self.process_list = \
            dict((n, []) for n in static_description.node_lookup.iterkeys())

    @ib.provides('infrastructure.name')
    def infra_name(self, infra_id, **kwargs):
        return self.static_description.name
    
    @ib.provides('infrastructure.static_description')
    def infra_descr(self, infra_id, **kwargs):
        return self.static_description

    @ib.provides('infrastructure.state')
    def infra_state(self, infra_id, **kwargs):
        return dict((k, len(v)) for (k,v) in self.process_list.iteritems())

statd = compiler.StaticDescription(config.infrastructure)
processor = SingletonLocalInfraProcessor(statd)
e = enactor.Enactor(infrastructure_id=statd.infra_id,
            infobroker=processor,
            infraprocessor=processor)
e.make_a_pass()
