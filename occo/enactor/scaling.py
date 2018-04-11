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

def get_scaling_limits(node):
    smin = max(node.get('scaling',dict()).get('min',1),1)
    smax = max(node.get('scaling',dict()).get('max',smin),smin)
    return smin, smax

def keep_limits_for_scaling(target_count, node):
    target_min, target_max = get_scaling_limits(node)
    target_count = int(max(target_count,target_min))
    target_count = min(target_count,target_max)
    return target_count

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

    target_min, target_max = get_scaling_limits(oneinstance['node_description'])
    target_count = keep_limits_for_scaling(target_count,oneinstance['node_description'])

    return dict(actual=count, target=target_count, min=target_min,
            max=target_max)

def get_act_target_count(node):
    nodename = node['name']
    infraid = node['infra_id']
    target_count = main_uds.get_scaling_target_count(infraid,nodename)
    target_count = keep_limits_for_scaling(target_count,node)
    return target_count

def process_create_node_requests(node, targetcount):
    nodename = node['name']
    infraid = node['infra_id']
    createnodes = main_uds.get_scaling_createnode(infraid, nodename)
    if len(createnodes.keys()) > 0:
        targetmin, targetmax = get_scaling_limits(node)
        targetcount += len(createnodes.keys())
        if targetcount > targetmax:
            log.warning('Scaling: request(s) ignored, maximum count (%i) reached for node \'%s\'', 
                         targetmax, nodename )
            targetcount = targetmax
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
        targetmin, targetmax = get_scaling_limits(node)
        targetcount -= len(destroynodes.keys())
        #remove all destroy requests below minimum
        if targetcount < targetmin:
            log.warning('Scaling: request(s) ignored, minimum count (%i) reached for node \'%s\'', 
                         targetmin, nodename )
            for keyid in destroynodes.keys()[:targetmin-targetcount]:
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
        targetmin, targetmax = get_scaling_limits(node)
        targetcount -= len(destroynodes.keys())
        #remove all destroy requests below minimum
        if targetcount < targetmin:
            log.warning('Scaling: request(s) ignored, minimum count (%i) reached for node \'%s\'', 
                         targetmin, nodename )
            for keyid in destroynodes.keys()[:targetmin-targetcount]:
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

def set_scalenode_request(infraid, nodename, count ):
    main_uds.set_scaling_target_count(infraid, nodename, count)
    return
