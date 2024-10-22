import math
from utils import *

earth_radius = 6378.135

altitude = 550
theta = calculate_range_handoffs(0, altitude, np.radians(25))

latency = 9

def get_hops(distance):
    if distance < 2 * (earth_radius + altitude) * math.sin(theta / 2):
        return 1
    elif distance < 2 * (earth_radius + altitude) * math.sin((2 * theta) / 2):
        return 1 * 3
    elif distance < 2 * (earth_radius + altitude) * math.sin((3 * theta) / 2):
        return 1 * 5
    elif distance < 2 * (earth_radius + altitude) * math.sin((4 * theta) / 2):
        return 1 * 7
    elif distance < 2 * (earth_radius + altitude) * math.sin((5 * theta) / 2):
        return 1 * 9
    elif distance < 2 * (earth_radius + altitude) * math.sin((6 * theta) / 2):
        return 1 * 11
    elif distance < 2 * (earth_radius + altitude) * math.sin((7 * theta) / 2):
        return 1 * 13
    elif distance < 2 * (earth_radius + altitude) * math.sin((8 * theta) / 2):
        return 1 * 15
    elif distance < 2 * (earth_radius + altitude) * math.sin((9 * theta) / 2):
        return 1 * 17
    elif distance < 2 * (earth_radius + altitude) * math.sin((10 * theta) / 2):
        return 1 * 19
    elif distance < 2 * (earth_radius + altitude) * math.sin((11 * theta) / 2):
        return 1 * 21
    elif distance < 2 * (earth_radius + altitude) * math.sin((12 * theta) / 2):
        return 1 * 23
    elif distance < 2 * (earth_radius + altitude) * math.sin((13 * theta) / 2):
        return 1 * 25
    elif distance < 2 * (earth_radius + altitude) * math.sin((14 * theta) / 2):
        return 1 * 27
    elif distance < 2 * (earth_radius + altitude) * math.sin((15 * theta) / 2):
        return 1 * 29
    elif distance < 2 * (earth_radius + altitude) * math.sin((16 * theta) / 2):
        return 1 * 31
    elif distance < 2 * (earth_radius + altitude) * math.sin((17 * theta) / 2):
        return 1 * 33
    elif distance < 2 * (earth_radius + altitude) * math.sin((18 * theta) / 2):
        return 1 * 35
    elif distance < 2 * (earth_radius + altitude) * math.sin((19 * theta) / 2):
        return 1 * 37
    elif distance < 2 * (earth_radius + altitude) * math.sin((20 * theta) / 2):
        return 1 * 39
    elif distance < 2 * (earth_radius + altitude) * math.sin((21 * theta) / 2):
        return 1 * 41
    elif distance < 2 * (earth_radius + altitude) * math.sin((22 * theta) / 2):
        return 1 * 43    
    else:
        return 1 * 45

def get_rtt(distance):
    return latency * get_hops(distance)
