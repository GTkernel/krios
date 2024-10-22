#!/usr/bin/env python

import time
from datetime import datetime
import random
import json
import numpy as np
import logging
import sys

from k8s import *
from utils import *
from kubernetes import client, config, watch

config.load_incluster_config()
api=client.CoreV1Api()
logger = logging.getLogger(__name__)
FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT, level=logging.DEBUG, filename='krios_scheduler.log')

satellites = read_tles("tles.txt")
elevation_angle = np.radians(25) # radians
altitude = 550 # km

def compute_node_positions(nodes, t) -> dict:
    node_positions = {}
    for node in nodes:
        sat_id = fetch_sat_id(node)
        l1 = satellites[sat_id]['line1']
        l2 = satellites[sat_id]['line2']
        satellite = Satrec.twoline2rv(l1, l2)
        jd, fr = jday(t.year, t.month, t.day, t.hour, t.minute, t.second)
        e, location, velocity = satellite.sgp4(jd, fr)
        logger.info(f"Node: {node.metadata.name}, sat_id: {sat_id}, location: {location}, time: {t}")
        node_positions[node.metadata.name] = location

    return node_positions

def fetch_sat_id(node):
    sat_id = node.metadata.labels.get('sat_id1', None)
    if not sat_id:
        sat_id = node.metadata.labels['sat_id']

    return int(sat_id)

# Fetch the nodes in the zone of the pod
def get_zone_nodes(pod, probe_time):
    filtered_nodes = []
    nodes = get_follower_nodes(api)
    node_positions = compute_node_positions(nodes, datetime.fromtimestamp(probe_time))
    radius = pod.metadata.labels.get('radius', 100)
    app_location = parseLocation(pod.metadata.labels['leozone'])
    
    app_x, app_y, app_z = geodetic2cartesian(app_location[0], app_location[1], altitude * 1000)
    allowable_distance = get_allowable_distance(radius, altitude, elevation_angle)
    logger.info(f"Get zone nodes for App location: {app_location} at time: {probe_time}")
    for node in nodes:
        # if the pod is already on the node, it wouldn't be used after the handover
        logger.info(f"Node: {node.metadata.name}, satid: {fetch_sat_id(node)}, node_location: {node_positions[node.metadata.name]}")
        if node.metadata.name == pod.spec.node_name:
            continue
        
        location = node_positions[node.metadata.name]
        distance = calculate_distance(location, (app_x, app_y, app_z))
        logger.info(f"Distance between node {node.metadata.name} with sat_id {fetch_sat_id(node)} and pod {pod.metadata.name}: {distance} while allowable distance is {allowable_distance}")
        if distance < allowable_distance:
            filtered_nodes.append(node)

    return filtered_nodes

# get the best node amongst the filtered nodes.
def get_best_node(pod, probe_time, filtered_nodes):
    app_location = parseLocation(pod.metadata.labels['leozone'])
    app_x, app_y, app_z = geodetic2cartesian(app_location[0], app_location[1], altitude * 1000)

    distances = {}
    krios_metrics = {}
    t = datetime.fromtimestamp(probe_time)
    jd, fr = jday(t.year, t.month, t.day, t.hour, t.minute, t.second)
    for node in filtered_nodes:
        sat_id = fetch_sat_id(node)
        l1 = satellites[sat_id]['line1']
        l2 = satellites[sat_id]['line2']
        satellite = Satrec.twoline2rv(l1, l2)
        e, node_location, node_velocity = satellite.sgp4(jd, fr)
        distances[node.metadata.name] = calculate_distance(node_location, (app_x, app_y, app_z))
        krios_metrics[node.metadata.name] = node_velocity[0] * (app_location[0] - node_location[0]) + node_velocity[1] * (app_location[1] - node_location[1]) + node_velocity[2] * (app_location[2] - node_location[2])
        logger.info(f"Node: {node.metadata.name}, sat_id: {sat_id}, distance: {distances[node.metadata.name]}, krios_metric: {krios_metrics[node.metadata.name]}")
    
    if len(krios_metrics) == 0:
        return None
    
    # return the key that has the best value in krios_metrics
    return max(krios_metrics, key=krios_metrics.get)

def scheduler(pod, namespace="default"):
    logger.info(f"Pod {pod.metadata.name} is in Pending state")
    if pod.spec.node_name is not None:
        best_node = pod.spec.node_name
    else:
        filtered_nodes = get_zone_nodes(pod, time.time())
        logger.info(f"Filtered nodes: {len(filtered_nodes)}")
        best_node = get_best_node(pod, time.time(), filtered_nodes)
        logger.info(f"Best node: {best_node}")

    if best_node is None:
        logger.error(f"No suitable node found for pod {pod.metadata.name}")
        return None

    vbtarget=client.V1ObjectReference(kind="Node", api_version="v1", name=best_node)
    
    meta=client.V1ObjectMeta(namespace=namespace, name=pod.metadata.name)
    body=client.V1Binding(metadata=meta, target=vbtarget)

    return api.create_namespaced_binding(namespace, body, _preload_content=False)

def main():
    w = watch.Watch()
    for event in w.stream(api.list_namespaced_pod, "default"):
        pod = event['object']
        logger.info(f"Event: {event['type']} Pod: {pod.metadata.name}")
        if pod.status.phase == "Pending":
            try:
                res = scheduler(pod)
            except client.rest.ApiException as e:
                logger.error(json.loads(e.body)['message'])
        
if __name__ == '__main__':
    logger.info("Krios Scheduler started")
    main()
