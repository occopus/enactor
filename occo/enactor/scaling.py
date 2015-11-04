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
Scaling related algorithms to be used by manager_service and enactor.

.. moduleauthor:: Jozsef Kovacs <jozsef.kovacs@sztaki.mta.hu>
"""

import occo.infobroker as ib
import logging
import occo.util as util

from occo.infobroker import main_uds

log = logging.getLogger('occo.scaling')
datalog = logging.getLogger('occo.data.scaling')


def report(instances):
    if not instances:
        raise Exception("Internal error: instances not found!")
    
    oneinstance = instances[instances.keys()[0]]
    infraid = oneinstance['infra_id']
    nodename = oneinstance['resolved_node_definition']['name']
    count = len(instances)
    
    target_count = int(util.coalesce(main_uds.get_scaling_target_count(infraid,nodename),
                    count))
    target_count += len(main_uds.get_scaling_createnode(infraid,nodename).keys())
    target_count -= len(main_uds.get_scaling_destroynode(infraid,nodename).keys())

    target_min = oneinstance['node_description'].get('scaling',dict()).get('min',1)
    target_max = oneinstance['node_description'].get('scaling',dict()).get('max',1)

    target_count = max(target_count,target_min)
    target_count = min(target_count,target_max)

    return dict(actual=count, target=target_count, min=target_min,
            max=target_max)

def get_act_target_count(node):
    nodename = node['name']
    infraid = node['infra_id']
    targetmin = node['scaling']['min']
    targetcount = int(util.coalesce(main_uds.get_scaling_target_count(infraid,nodename),
                    targetmin))
    return targetcount

def process_create_node_requests(node, targetcount):
    nodename = node['name']
    infraid = node['infra_id']
    createnodes = main_uds.get_scaling_createnode(infraid, nodename)
    if len(createnodes.keys()) > 0:
        targetmax = node['scaling']['max']
        targetcount += len(createnodes.keys())
        targetcount = min(targetcount,targetmax)
        for keyid in createnodes.keys():
            main_uds.del_scaling_createnode(infraid,nodename,keyid)
        main_uds.set_scaling_target_count(infraid,nodename,targetcount)
    return targetcount

def remove_create_node_requests(infraid, nodename, requests):
    return

def process_drop_node_requests_with_ids(node, targetcount):
    nodename = node['name']
    infraid = node['infra_id']
    dnlist = main_uds.get_scaling_destroynode(infraid,nodename)
    destroynodes = dict()
    for keyid, nodeid in dnlist.iteritems():
        if nodeid != "":
            destroynodes[keyid]=nodeid
    if len(destroynodes.keys()) > 0:
        targetmin = node['scaling']['min']
        targetcount -= len(destroynodes.keys())
        #remove all destroy requests below minimum
        if targetcount < targetmin:
            for keyid in destroynodes.keys()[:targetmin-targetcount]:
                log.info('SCALING: %i DROP node request(s) ignored, minimum reached', targetmin - targetcount )
                main_uds.del_scaling_destroynode(infraid,nodename,keyid)
        targetcount = max(targetcount,targetmin)
        main_uds.set_scaling_target_count(infraid,nodename,targetcount)
    return targetcount

def process_drop_node_requests_with_no_ids(node, targetcount):
    nodename = node['name']
    infraid = node['infra_id']
    dnlist = main_uds.get_scaling_destroynode(infraid,nodename)
    destroynodes = dict()
    for keyid, nodeid in dnlist.iteritems():
        if nodeid == "":
            destroynodes[keyid]=nodeid
    if len(destroynodes.keys()) > 0:
        targetmin = node['scaling']['min']
        targetcount -= len(destroynodes.keys())
        #remove all destroy requests below minimum
        if targetcount < targetmin:
            for keyid in destroynodes.keys()[:targetmin-targetcount]:
                log.info('SCALING: %i DROP node request(s) ignored, minimum reached', targetmin - targetcount )
                main_uds.del_scaling_destroynode(infraid,nodename,keyid)
        targetcount = max(targetcount,targetmin)
        main_uds.set_scaling_target_count(infraid,nodename,targetcount)
    return targetcount

def add_createnode_request(infraid, nodename, count = 1):
    main_uds.set_scaling_createnode(infraid, nodename, count)
    return

def add_dropnode_request(infraid, nodename, nodeid):
    main_uds.set_scaling_destroynode(infraid, nodename, nodeid)
    return

