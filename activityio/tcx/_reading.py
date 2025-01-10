#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re
import dateutil
from datetime import datetime as dt

import pandas as pd
from pandas import to_datetime

from activityio._types import ActivityData, special_columns
from activityio._util import drydoc, exceptions
from activityio._util.xml_reading import (
    gen_nodes, recursive_text_extract, sans_ns)


CAP = re.compile(r'([A-Z]{1})')

# According to Garmin, all times are stored in UTC.
DATETIME_FMT = '%Y-%m-%dT%H:%M:%SZ'
# Despite what the schema says, there are files out
# in the wild with fractional seconds...
DATETIME_FMT_WITH_FRAC = '%Y-%m-%dT%H:%M:%S.%fZ'

COLUMN_SPEC = {
    'altitude_meters': special_columns.Altitude,
    'cadence': special_columns.Cadence,
    'distance_meters': special_columns.Distance,
    'longitude_degrees': special_columns.Longitude,
    'latitude_degrees': special_columns.Latitude,
    'speed': special_columns.Speed,
    'watts': special_columns.Power,
}


def titlecase_to_undercase(string):
    """ ColumnName --> column_name """
    under = CAP.sub(lambda pattern: '_' + pattern.group(1).lower(), string)
    return under.lstrip('_')


@drydoc.gen_records
def gen_records(file_path):

    nodes = gen_nodes(file_path, ('Trackpoint',), with_root=True)

    root = next(nodes)
    if sans_ns(root.tag) != 'TrainingCenterDatabase':
        raise exceptions.InvalidFileError('tcx')

    trackpoints = nodes
    for trkpt in trackpoints:
        yield recursive_text_extract(trkpt)


@drydoc.gen_records
def gen_records_trk_crs(file_path):

    trk_pts = []
    crs_pts = []

    nodes = gen_nodes(file_path, ('Trackpoint',), with_root=True)

    root = next(nodes)
    if sans_ns(root.tag) != 'TrainingCenterDatabase':
        raise exceptions.InvalidFileError('tcx')

    trackpoints = nodes
    for trkpt in trackpoints:
        trk_pts.append(recursive_text_extract(trkpt))

    try:
        nodes = gen_nodes(file_path, ('CoursePoint',), with_root=True)

        root = next(nodes)
        if sans_ns(root.tag) != 'TrainingCenterDatabase':
            raise exceptions.InvalidFileError('tcx')

        coursepoints = nodes
        for crspt in coursepoints:
            crs_pts.append(recursive_text_extract(crspt))

    except:
        pass

    return trk_pts if len(trk_pts)!=0 else None, crs_pts if len(crs_pts)!=0 else None


def read_and_format(file_path):

    def post_processing(data):
        data = ActivityData.from_records(data)
        times = data.pop('Time')  # should always be there
        # data = data.astype(copy=False)   # try and make numeric

        # Prettier column names!
        data.columns = map(titlecase_to_undercase, data.columns)

        # try:
        #     timestamps = to_datetime(times, format=DATETIME_FMT, utc=True)
        # except ValueError:  # bad format, try with fractional seconds
        #     timestamps = to_datetime(times, format=DATETIME_FMT_WITH_FRAC, utc=True)

        timestamps = times.apply(lambda x: dt.timestamp(dateutil.parser.parse(x)))
        timeoffsets = timestamps - timestamps[0]
        data._finish_up(column_spec=COLUMN_SPEC,
                        start=timestamps[0], timeoffsets=timeoffsets)
        data['Time'] = timestamps.to_list()

        return data

    trk_pts, crs_pts = gen_records_trk_crs(file_path)

    if trk_pts is not None:
        trk_pts = post_processing(trk_pts)
        trk_pts = pd.DataFrame(trk_pts).rename(columns={'LatitudeDegrees': 'lat', 'LongitudeDegrees': 'lon', 'AltitudeMeters': 'elev', 'DistanceMeters': 'dist', 'Time': 'time'})

    if crs_pts is not None:
        crs_pts = post_processing(crs_pts)
        crs_pts = pd.DataFrame(crs_pts).rename(columns={'Name': 'name', 'LatitudeDegrees': 'lat', 'LongitudeDegrees': 'lon', 'PointType': 'type', 'Notes': 'note', 'Time': 'time'})

    return trk_pts, crs_pts
