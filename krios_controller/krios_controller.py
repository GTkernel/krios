import math
import random
import threading
import time
from datetime import datetime
import logging
import sys
import os

from kubernetes import client, config
from sgp4.api import Satrec, jday

from k8s import *
from latency import get_rtt
from utils import *

earth_radius = 6378.135
altitude = 550
theta = math.acos(earth_radius / (earth_radius + altitude))
satellites = read_tles("tles.txt")
altitude = 550 # km
elevation_angle = np.radians(25) # radians


config.load_incluster_config()
api = client.CoreV1Api()

logger = logging.getLogger(__name__)
FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO, filename='krios_controller.log')
# fill in the latitude, logitude and elevation for your ground station
# I'm using Oregon here.
ground_station = (38.875, -121.707056, 0)

def fetch_sat_id(node):
    sat_id = node.metadata.labels.get('sat_id1', None)
    if not sat_id:
        sat_id = node.metadata.labels['sat_id']

    return int(sat_id)
    

def compute_node_positions(nodes, t) -> dict:
    node_positions = {}
    for node in nodes:
        sat_id = fetch_sat_id(node)
        l1 = satellites[sat_id]['line1']
        l2 = satellites[sat_id]['line2']
        satellite = Satrec.twoline2rv(l1, l2)
        jd, fr = jday(t.year, t.month, t.day, t.hour, t.minute, t.second)
        e,location, velocity = satellite.sgp4(jd, fr)

        node_positions[node.metadata.name] = location

    return node_positions


# Fetch the nodes in the zone of the pod
def get_zone_nodes(pod, handover_time):
    filtered_nodes = []
    nodes = get_follower_nodes(api)
    node_positions = compute_node_positions(nodes, datetime.fromtimestamp(handover_time))
    radius = pod.metadata.labels.get('radius', 100)
    app_location = parseLocation(pod.metadata.labels['leozone'])
    
    app_x, app_y, app_z = geodetic2cartesian(app_location[0], app_location[1], altitude * 1000)
    allowable_distance = get_allowable_distance(radius, altitude, elevation_angle)
    logger.info(f"Get zone nodes for App location: {app_location} at time: {handover_time}")
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
def get_best_node(pod, handover_time, filtered_nodes, random_node=False, closest_node=False):
    if len(filtered_nodes) == 0:
        return None
    
    if random_node:
        return random.choice(filtered_nodes)

    
    app_location = parseLocation(pod.metadata.labels['leozone'])
    app_x, app_y, app_z = geodetic2cartesian(app_location[0], app_location[1], altitude * 1000)

    distances = {}
    krios_metrics = {}

    t = datetime.fromtimestamp(handover_time)
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

    if closest_node:
        return min(distances.items(), key=distances.get)
    
    return max(krios_metrics.items(), key=krios_metrics.get)

# Identify when this satellite will on longer be accessible from the zone
def node_leaving_zone(curr_time: float, pod, node) -> float:
    last_visible_time = curr_time
    sat_id = fetch_sat_id(node)
    l1 = satellites[sat_id]['line1']
    l2 = satellites[sat_id]['line2']
    satellite = Satrec.twoline2rv(l1, l2)

    radius = pod.metadata.labels.get('radius', 100)
    app_location = parseLocation(pod.metadata.labels['leozone'])
    app_x, app_y, app_z = geodetic2cartesian(app_location[0], app_location[1], altitude * 1000)
    allowable_distance = get_allowable_distance(radius, altitude, elevation_angle)

    probe_time = curr_time + 100
    out_of_bounds_time = curr_time + 1000
    while True:
        dt = datetime.fromtimestamp(probe_time)
        jd, fr = jday(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
        _, location, _ = satellite.sgp4(jd, fr)
        distance = calculate_distance(location, (app_x, app_y, app_z))
        if distance > allowable_distance:
            out_of_bounds_time = probe_time
            break

        last_visible_time = probe_time
        probe_time += 100

    # do binary search to find the exact time
    while out_of_bounds_time - last_visible_time > 1:
        probe_time = (out_of_bounds_time + last_visible_time) / 2
        dt = datetime.fromtimestamp(probe_time)
        jd, fr = jday(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
        e, location, velocity = satellite.sgp4(jd, fr)
        distance = calculate_distance(location, (app_x, app_y, app_z))
        if distance > allowable_distance:
            out_of_bounds_time = probe_time
        else:
            last_visible_time = probe_time

    return last_visible_time

def handover_manager(pod, new_node):
    pod_name = pod.metadata.name
    if(pod_name.endswith(pod.spec.node_name)):
        pod_name = pod_name[0:len(pod_name) - 1 - len(pod.spec.node_name)]
    new_pod_name = pod_name + "-" + new_node.metadata.name

    
    new_pod = create_new_pod(api, pod, new_pod_name, new_node.metadata.name)
    pod_node = new_pod.spec.node_name
    logger.info(f"Pod {new_pod.metadata.name} created on node {pod_node}")
    
    while not is_pod_ready(api, new_pod):
        new_pod = get_pod(api, new_pod.metadata.name, new_pod.metadata.namespace)
        time.sleep(1)

    delete_pod(api, pod)

# Sleep until the satellite is moving out of the zone, and then trigger the handover process
def pod_manager(pod, node, sleep_time, handover_time):
    if sleep_time > 0:
        time.sleep(sleep_time)
    
    best_node = None
    logger.info(f"Finding best node for pod {pod.metadata.name}")
    filtered_nodes = get_zone_nodes(pod, handover_time)
    logger.info(f"Filtered nodes: {filtered_nodes}")
    best_node = get_best_node(pod, handover_time, filtered_nodes)
    logger.info(f"Best node: {best_node}")

    if best_node is None:
        logger.error(f"No best node found for pod {pod.metadata.name}")
        return
    
    handover_manager(pod, best_node)

def controller_loop(lookahead=True, random_node=False, closest_node=False):
    active_pods = []

    tt = round(time.time())
    existing_pods = set()
    while True:
        # sleep until the next second
        if time.time() < tt:
            time.sleep(tt - time.time())

        logger.info(f"starting new iteration at time {tt}")
        nodes = get_follower_nodes(api)
        node_positions = compute_node_positions(nodes, datetime.fromtimestamp(tt))
        gs_x, gs_y, gs_z = geodetic2cartesian(ground_station[0], ground_station[1], 550000)
        logger.info(f"node positions computed at {time.time()}")

        pods = get_pods(api)
        logger.info(f"pods fetched at {time.time()}")
        if len(pods) == 0:
            tt += 1
            continue

        for pod in pods:
            if pod.metadata.name in existing_pods:
                continue

            if not is_pod_ready(api, pod):
                continue

            logger.info(f"Processing pod {pod.metadata.name}")
            existing_pods.add(pod.metadata.name)
            pod_node = next((node for node in nodes if node.metadata.name == pod.spec.node_name), None)
            if pod_node is None:
                logger.error(f"Pod {pod.metadata.name} is not on any node. It's node name is {pod.spec.node_name}")
                continue

            node_leaving_time = node_leaving_zone(tt, pod, pod_node)
            logger.info(f"Node leaving time computed {node_leaving_time}")

            if lookahead:
                just_ahead_time = 5 + 0.001 * get_rtt(calculate_distance((gs_x, gs_y, gs_z), node_positions[pod_node.metadata.name])) + (3000 / 7575) * (1 / (24 * 60 * 60))
            else:
                just_ahead_time = 0

            p = threading.Thread(target=pod_manager, args=(pod, pod_node, node_leaving_time - tt - just_ahead_time, just_ahead_time))
            p.start()



        tt += 1      

def main():
    controller_loop(lookahead=True, random_node=False, closest_node=False)

if __name__ == '__main__':
    main()