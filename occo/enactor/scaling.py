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
    target_count = int(target_count)
    target_count = max(target_count,target_min)
    target_count = min(target_count,target_max)
    return target_count

def report(instances):
    if not instances:
        raise Exception("Internal error: instances not found!")

    oneinstance = instances[list(instances.keys())[0]]
    infraid = oneinstance['infra_id']
    nodename = oneinstance['resolved_node_definition']['name']
    count = len(instances)

    target_count = int(util.coalesce(main_uds.get_scaling_target_count(infraid,nodename),
                    count))
    target_count += len(list(main_uds.get_scaling_createnode(infraid,nodename).keys()))
    target_count -= len(list(main_uds.get_scaling_destroynode(infraid,nodename).keys()))

    target_min, target_max = get_scaling_limits(oneinstance['node_description'])
    target_count = keep_limits_for_scaling(target_count,oneinstance['node_description'])

    return dict(actual=count, target=target_count, min=target_min,
            max=target_max)

def get_act_target_count(node):
    nodename = node['name']
    infraid = node['infra_id']
    target_count = main_uds.get_scaling_target_count(infraid,nodename)
    target_count = 0 if target_count is None else target_count
    target_count = keep_limits_for_scaling(target_count,node)
    return target_count

def process_create_node_requests(node, targetcount):
    nodename = node['name']
    infraid = node['infra_id']
    createnodes = main_uds.get_scaling_createnode(infraid, nodename)
    if len(list(createnodes.keys())) > 0:
        targetmin, targetmax = get_scaling_limits(node)
        targetcount += len(list(createnodes.keys()))
        if targetcount > targetmax:
            log.warning('Scaling: request(s) ignored, maximum count (%i) reached for node \'%s\'',
                         targetmax, nodename )
            targetcount = targetmax
        for keyid in list(createnodes.keys()):
            main_uds.del_scaling_createnode(infraid,nodename,keyid)
        main_uds.set_scaling_target_count(infraid,nodename,targetcount)
    return targetcount

def remove_create_node_requests(infraid, nodename, requests):
    return

def process_drop_node_requests_with_ids(node, targetcount, dynamic_state):
    nodename = node['name']
    infraid = node['infra_id']
    #log.info('Calling process_drop_node_requests_with_ids({0}{1})...'.format(infraid,nodename))
    #log.info('        targetcount: {0}'.format(targetcount))

    existing_nodes_with_ips = {}
    existing_ips_with_nodes = {}
    for _,instance, in list(dynamic_state.items()):
      existing_nodes_with_ips[instance['node_id']]=instance['resource_address']
      if isinstance(instance['resource_address'], list):
        for nra in instance['resource_address']:
          existing_ips_with_nodes[nra]=instance['node_id']
      else:
        existing_ips_with_nodes[instance['resource_address']]=instance['node_id']
    #log.info('EXISTING NODES WITH IPS: {0}'.format(existing_nodes_with_ips))
    #log.info('EXISTING IPS WITH NODES: {0}'.format(existing_ips_with_nodes))

    #Collecting nodeids and requestids
    dnlist = main_uds.get_scaling_destroynode(infraid,nodename)
    #log.info('DNLIST: {0}'.format(dnlist))
    request_ids_with_destroy_node_id = dict()
    destroy_node_ids_with_request_id = dict()
    for keyid, nodeid in list(dnlist.items()):
      if nodeid != "":
        #Convert ipaddress to nodeid
        if nodeid.count('.')>2:
          main_uds.del_scaling_destroynode(infraid,nodename,keyid)
          if nodeid in list(existing_ips_with_nodes):
            ipaddress = nodeid
            nodeid = existing_ips_with_nodes[ipaddress]
            keyid = main_uds.set_scaling_destroynode(infraid, nodename, nodeid)
            request_ids_with_destroy_node_id[keyid]=nodeid
            destroy_node_ids_with_request_id[nodeid]=keyid
        #Check if nodid is valid
        elif nodeid in list(existing_nodes_with_ips):
          request_ids_with_destroy_node_id[keyid]=nodeid
          destroy_node_ids_with_request_id[nodeid]=keyid
        else:
          main_uds.del_scaling_destroynode(infraid,nodename,keyid)
    #log.info('REQUEST_IDS_WITH_DESTROY_NODE_ID: {0}'.format(request_ids_with_destroy_node_id))
    #log.info('DESTROY_NODE_IDS_WITH_REQUEST_ID: {0}'.format(destroy_node_ids_with_request_id))
    if len(list(request_ids_with_destroy_node_id.keys())) > 0:
        targetmin, targetmax = get_scaling_limits(node)
        targetcount -= len(list(request_ids_with_destroy_node_id.keys()))
        #remove all destroy requests below minimum
        if targetcount < targetmin:
            log.warning('Scaling: request(s) ignored, minimum count (%i) reached for node \'%s\'',
                         targetmin, nodename )
            for keyid in list(request_ids_with_destroy_node_id.keys())[:targetmin-targetcount]:
                main_uds.del_scaling_destroynode(infraid,nodename,keyid)
        targetcount = max(targetcount,targetmin)
        main_uds.set_scaling_target_count(infraid,nodename,targetcount)
    return targetcount

def process_drop_node_requests_with_no_ids(node, targetcount):
    nodename = node['name']
    infraid = node['infra_id']
    dnlist = main_uds.get_scaling_destroynode(infraid,nodename)
    destroynodes = dict()
    for keyid, nodeid in list(dnlist.items()):
        if nodeid == "":
            destroynodes[keyid]=nodeid
    if len(list(destroynodes.keys())) > 0:
        targetmin, targetmax = get_scaling_limits(node)
        targetcount -= len(list(destroynodes.keys()))
        #remove all destroy requests below minimum
        if targetcount < targetmin:
            log.warning('Scaling: request(s) ignored, minimum count (%i) reached for node \'%s\'',
                         targetmin, nodename )
            for keyid in list(destroynodes.keys())[:targetmin-targetcount]:
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
