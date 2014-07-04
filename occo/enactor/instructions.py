#
# Copyright (C) 2014 MTA SZTAKI
#

# TODO: think this through
# TODO: this should be moved to a common lib, or an infraprocessor sub-module
class IPInstruction(dict):
    def __init__(self, instruction, **kwargs):
        self.update(kwargs)
        self.__dict__ = self
        self.instruction = instruction
class IPStartNode(IPInstruction):
    def __init__(self, node):
        super(IPStartNode, self).__init__(instruction='start', node=node)
    def __str__(self):
        return '{%s -> %s}'%(self.instruction, self.node['name'])
class IPStopNode(IPInstruction):
    def __init__(self, node_id):
        super(IPStopNode, self).__init__(instruction='drop', node_id=node_id)
    def __str__(self):
        return '{%s -> %s}'%(self.instruction, self.node_id)
class IPCreateEnvironment(IPInstruction):
    def __init__(self, environment_id):
        super(IPCreateEnvironment, self).__init__(instruction='create_enviro',
                                                  enviro_id=environment_id)
    def __str__(self):
        return '{%s -> %s}'%(self.instruction, self.enviro_id)
# etc
