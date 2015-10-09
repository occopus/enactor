#
# Copyright (C) 2015 MTA SZTAKI
#

"""
Scaling related algorithms to be used by manager_service and enactor.

.. moduleauthor:: Jozsef Kovacs <jozsef.kovacs@sztaki.mta.hu>
"""

import occo.infobroker as ib
import logging
import occo.util as util

from occo.infobroker import main_uds

log = logging.getLogger('occo.scaling')
datalog = logging.getLogger('occo.data.scaling')


def report_target_count(instances):
    if not instances:
        return 1
    
    oneinstance = instances[instances.keys()[0]]
    infraid = oneinstance['infra_id']
    nodename = oneinstance['resolved_node_definition']['name']
    count = len(instances)
    
    target_count = int(util.coalesce(main_uds.get_scaling_target_count(infraid,nodename),
                    count))
    target_count += len(main_uds.get_scaling_createnode(infraid,nodename))
    target_count -= len(main_uds.get_scaling_destroynode(infraid,nodename))

    target_min = oneinstance['node_description'].get('scaling',dict()).get('min',1)
    target_max = oneinstance['node_description'].get('scaling',dict()).get('max',1)

    target_count = max(target_count,target_min)
    target_count = min(target_count,target_max)

    return target_count

def get_act_target_count(node):
    nodename = node['name']
    infraid = node['infra_id']
    targetmin = node['scaling']['min']
    targetcount = main_uds.get_scaling_target_count(infraid,nodename)
    targetcount = int(util.coalesce(targetcount, targetmin))
    return targetcount

def process_create_node_requests(node, targetcount):
    nodename = node['name']
    infraid = node['infra_id']
    createnodes = main_uds.get_scaling_createnode(infraid, nodename)
    if len(createnodes) > 0:
        targetmax = node['scaling']['max']
        targetcount += len(createnodes)
        targetcount = min(targetcount,targetmax)
        for nodeid in createnodes:
            main_uds.del_scaling_createnode(infraid,nodename,nodeid)
            main_uds.set_scaling_target_count(infraid,nodename,targetcount)
    return targetcount

def remove_create_node_requests(infraid, nodename, requests):
    return

def process_drop_node_requests(node, targetcount):
    nodename = node['name']
    infraid = node['infra_id']
    destroynodes = main_uds.get_scaling_destroynode(infraid,nodename)
    if len(destroynodes) > 0:
        targetmin = node['scaling']['min']
        targetcount -= len(destroynodes)
        #remove all destroy requests below minimum
        if targetcount < targetmin:
            for nodeid in destroynodes[:targetmin-targetcount]:
                main_uds.del_scaling_destroynode(infraid,nodename,nodeid)
        targetcount = max(targetcount,targetmin)
    return targetcount

def add_createnode_request(infraid, nodename):
    main_uds.set_scaling_createnode(infraid, nodename)
    return

def add_dropnode_request(infraid, nodename, nodeid):
    main_uds.set_scaling_destroynode(infraid, nodename, nodeid)
    return

