import math
from sgp4.exporter import export_tle
from sgp4.api import Satrec, WGS72
from sgp4.api import jday
import numpy as np

earth_radius = 6378.135
altitude = 550
theta = math.acos(earth_radius / (earth_radius + altitude))

def generate_tles_from_scratch_with_sgp(
        filename_out,
        constellation_name,
        num_orbits,
        num_sats_per_orbit,
        phase_diff,
        inclination_degree,
        eccentricity,
        arg_of_perigee_degree,
        mean_motion_rev_per_day
):

    with open(filename_out, "w+") as f_out:

        # First line:
        #
        # <number of orbits> <number of satellites per orbit>
        #
        f_out.write("%d %d\n" % (num_orbits, num_sats_per_orbit))

        # Each of the subsequent (number of orbits * number of satellites per orbit) blocks
        # define a satellite as follows:
        #
        # <constellation_name> <global satellite id>
        # <TLE line 1>
        # <TLE line 2>
        satellite_counter = 0
        for orbit in range(0, num_orbits):

            # Orbit-dependent
            raan_degree = orbit * 360.0 / num_orbits
            orbit_wise_shift = 0
            if orbit % 2 == 1:
                if phase_diff:
                    orbit_wise_shift = 360.0 / (num_sats_per_orbit * 2.0)

            # For each satellite in the orbit
            for n_sat in range(0, num_sats_per_orbit):
                mean_anomaly_degree = orbit_wise_shift + (n_sat * 360 / num_sats_per_orbit)

                # Epoch is set to the year 2000
                # This conveniently in TLE format gives 00001.00000000
                # for the epoch year and Julian day fraction entry
                jd, fr = jday(2000, 1, 1, 0, 0, 0)

                # Use SGP-4 to generate TLE
                sat_sgp4 = Satrec()

                # Based on: https://pypi.org/project/sgp4/
                sat_sgp4.sgp4init(
                    WGS72,                  # Gravity model [1]
                    'i',                    # Operating mode (a = old AFPSC mode, i = improved mode)
                    satellite_counter + 1,  # satnum:  satellite number
                    (jd + fr) - 2433281.5,  # epoch:   days since 1949 December 31 00:00 UT [2]
                    0.0,                    # bstar:   drag coefficient (kg/m2er)
                    0.0,                    # ndot:    ballistic coefficient (revs/day)
                    0.0,                    # nndot:   second derivative of mean motion (revs/day^3)
                    eccentricity,           # ecco:    eccentricity
                    math.radians(arg_of_perigee_degree),              # argpo:   argument or perigee (radians)
                    math.radians(inclination_degree),                 # inclo:    inclination(radians)
                    math.radians(mean_anomaly_degree),                # mo:       mean anomaly (radians)
                    mean_motion_rev_per_day * 60 / 13750.9870831397,  # no_kazai: mean motion (radians/minute) [3]
                    math.radians(raan_degree)                         # nodeo:    right ascension of
                                                                      #           ascending node (radians)
                )

                # Side notes:
                # [1] WGS72 is also used in the NS-3 model
                # [2] Due to a bug in sgp4init, the TLE below irrespective of the value here gives zeros.
                # [3] Conversion factor from:
                #     https://www.translatorscafe.com/unit-converter/en-US/velocity-angular/1-9/radian/second-revolution/day/
                #

                # Export TLE from the SGP-4 object
                line1, line2 = export_tle(sat_sgp4)

                # Line 1 has some problems: there are unknown characters entered for the international
                # designator, and the Julian date is not respected
                # As such, we set our own bogus international designator 00000ABC
                # and we set our own epoch date as 1 January, 2000
                # Why it's 00001.00000000: https://www.celestrak.com/columns/v04n03/#FAQ04
                tle_line1 = line1[:7] + "U 00000ABC 00001.00000000 " + line1[33:]
                tle_line1 = tle_line1[:68] + str(calculate_tle_line_checksum(tle_line1[:68]))
                tle_line2 = line2

                # Check that the checksum is correct
                if len(tle_line1) != 69 or calculate_tle_line_checksum(tle_line1[:68]) != int(tle_line1[68]):
                    raise ValueError("TLE line 1 checksum failed")
                if len(tle_line2) != 69 or calculate_tle_line_checksum(tle_line2[:68]) != int(tle_line2[68]):
                    raise ValueError("TLE line 2 checksum failed")

                # Write TLE to file
                f_out.write(constellation_name + " " + str(orbit * num_sats_per_orbit + n_sat) + "\n")
                f_out.write(tle_line1 + "\n")
                f_out.write(tle_line2 + "\n")

                # One more satellite there
                satellite_counter += 1

def calculate_tle_line_checksum(tle_line_without_checksum):
    if len(tle_line_without_checksum) != 68:
        raise ValueError("Must have exactly 68 characters")
    s = 0
    for i in range(len(tle_line_without_checksum)):
        if tle_line_without_checksum[i].isnumeric():
            s += int(tle_line_without_checksum[i])
        if tle_line_without_checksum[i] == "-":
            s += 1
    return s % 10


def geodetic2cartesian(lat_degrees, lon_degrees, ele_m):
    """
    Compute geodetic coordinates (latitude, longitude, elevation) to Cartesian coordinates.

    :param lat_degrees: Latitude in degrees (float)
    :param lon_degrees: Longitude in degrees (float)
    :param ele_m:  Elevation in meters

    :return: Cartesian coordinate as 3-tuple of (x, y, z)
    """

    #
    # Adapted from: https://github.com/andykee/pygeodesy/blob/master/pygeodesy/transform.py
    #

    # WGS72 value,
    # Source: https://geographiclib.sourceforge.io/html/NET/NETGeographicLib_8h_source.html
    a = 6378135.0

    # Ellipsoid flattening factor; WGS72 value
    # Taken from https://geographiclib.sourceforge.io/html/NET/NETGeographicLib_8h_source.html
    f = 1.0 / 298.26

    # First numerical eccentricity of ellipsoid
    e = math.sqrt(2.0 * f - f * f)
    lat = lat_degrees * (math.pi / 180.0)
    lon = lon_degrees * (math.pi / 180.0)

    # Radius of curvature in the prime vertical of the surface of the geodetic ellipsoid
    v = a / math.sqrt(1.0 - e * e * math.sin(lat) * math.sin(lat))

    x = (v + ele_m) * math.cos(lat) * math.cos(lon)
    y = (v + ele_m) * math.cos(lat) * math.sin(lon)
    z = (v * (1.0 - e * e) + ele_m) * math.sin(lat)

    return x / 1000, y / 1000, z / 1000


def read_tles(filename_tles):
    """
    Read a constellation of satellites from the TLES file.

    :param filename_tles:                    Filename of the TLES (typically /path/to/tles.txt)

    :return: Dictionary: {
                    "n_orbits":             Number of orbits
                    "n_sats_per_orbit":     Satellites per orbit
                    "epoch":                Epoch
                    "satellites":           Dictionary of satellite id to
                                            {"ephem_obj_manual": <obj>, "ephem_obj_direct": <obj>}
              }
    """
    satellites = []
    with open(filename_tles, 'r') as f:
        universal_epoch = None
        for tles_line_1 in f:
            tles_line_2 = f.readline()
            tles_line_3 = f.readline()

            # Retrieve name and identifier
            name = tles_line_1

            satellite = {}
            satellite['line1'] = tles_line_2
            satellite['line2'] = tles_line_3

            satellites.append(satellite)

    return satellites

def read_starlink_tles(filename_tles):
    """
    Read a constellation of satellites from the TLES file.

    :param filename_tles:                    Filename of the TLES (typically /path/to/tles.txt)

    :return: Dictionary: {
                    "n_orbits":             Number of orbits
                    "n_sats_per_orbit":     Satellites per orbit
                    "epoch":                Epoch
                    "satellites":           Dictionary of satellite id to
                                            {"ephem_obj_manual": <obj>, "ephem_obj_direct": <obj>}
              }
    """
    satellites = []
    with open(filename_tles, 'r') as f:
        i = 0
        for tles_line_1 in f:
            tles_line_2 = f.readline()
            tles_line_3 = f.readline()

            # Retrieve name and identifier
            name = tles_line_1
            i += 1

            satellite = {}
            satellite['line1'] = tles_line_2
            satellite['line2'] = tles_line_3

            satellites.append(satellite)

    return satellites

def calculate_distance(point1, point2):
    return math.sqrt((point2[0] - point1[0]) * (point2[0] - point1[0]) 
        + (point2[1] - point1[1]) * (point2[1] - point1[1])
     + (point2[2] - point1[2]) * (point2[2] - point1[2]))

def parseLocation(location):
    lat,long = location.split('_')

    return float(lat), float(long), 0

def get_allowable_distance(app_radius, altitude, elevation_angle):
    phi = calculate_range_handoffs(app_radius, altitude, elevation_angle)
    return 2 * (earth_radius + altitude) * math.sin(phi / 2)

def haversine_distance(p1, p2):
    const = 2 * earth_radius
    p1 = (math.radians(p1[0]), math.radians(p1[1]))
    p2 = (math.radians(p2[0]), math.radians(p2[1]))
    
    dlon = p1[1] - p2[1]
    dlat = p1[0] - p2[0]
    return const * math.asin(math.sqrt(math.sin(dlat / 2) ** 2 + math.cos(p1[0]) * math.cos(p2[0]) * math.sin(dlon / 2) ** 2))

def calculate_range_handoffs(radius, altitude, elevation_angle):
    region_angle = radius / earth_radius
    angle_at_orbit = np.arcsin((np.sin(np.pi / 2 + elevation_angle) * earth_radius) / (altitude + earth_radius) )
    
    angle_at_center = np.pi / 2 - elevation_angle - angle_at_orbit
    phi = angle_at_center - region_angle

    return phi