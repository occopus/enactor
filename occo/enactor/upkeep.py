#
# Copyright (C) 2015 MTA SZTAKI
#

"""
Upkeep algorithms to be used before making an Enactor pass.

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>
"""

import occo.infobroker as ib
import occo.util.factory as factory

class Upkeep(factory.MultiBackend):
    def __init__(self):
        self.infobroker = ib.main_info_broker

    def acquire_dynamic_state(self, infra_id):
        raise NotImplementedError()

@factory.register(Upkeep, 'noop')
class DefaultUpkeep(Upkeep):
    def acquire_dynamic_state(self, infra_id):
        return self.infobroker.get('infrastructure.state', infra_id)
