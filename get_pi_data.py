"""
Script for extracting time series data from an OSI PI database
==============================================================

Requirements
------------
1. User needs to have python 3 installed
2. User needs access rights to the PI database (For Equinor users, see accessIT)
3. Equinor users need PI Network Manager running on their local machine.

Preparation before running
--------------------------
Create a list of tags you want to extract in a file. The name of the file can
be specified as a command line argument but the default name is "tag.list".
One tag per line. Use say PI-Explorer to see a list of available tags.


Examples of usage:
==================
For the tags in the tag.list file, the following extracts time series 
between 12:00 and 12:10 on the 7th of April 2019.

Using windowing option ‘se’
---------------------------
The windowing option “se” expects the user to provide 2 additional positioning 
arguments s and e, which represents the start and end datetimes for windowing.
The key here is to wrap each datetime in " " and to avoid ambiguous datetimes.

python fetchTags.py se "07.Apr.2019  12:00:00" "07.Apr.2019  12:10:00"
python fetchTags.py se "Apr.07.2019  12:00:00" "12:10:00 Apr.2019.07"
python fetchTags.py se "12:00 April.07.2019 " "12:10:00 2019.07.April"
python fetchTags.py se "12:00 April/07/2019 " "12:10:00 2019/07/April"

 
Using windowing option 'sw'
---------------------------
The windowing option “sw” expects the user to provide 1 positional argument,
which is the start time, and one optional argument, which is the time window.
As with the se option, the positional argument can be expressed in all 
reasonable datetime formats. The time window is optional because it defaults to 1 day. 
The optional argument flagged by -w has to take the following format 

(<N1>d)(<N2>hrs)(<N3>m)(<N4>s)

where N1 N2, N3, and N4 represent a number, and d=days, hrs=hours, m=mins, s=seconds. 
At least one the bracketed items should be included if you specify the -w option. 
The following are all valid (note that the bracket themselves should be omitted)

python fetchTags.py sw -w “2d10m” "12:00 April.07.2019 "
python fetchTags.py sw -w “2d2hrs5m” "12:00 April.07.2019 "

To extract the time series in the example (ref 'se' case) you would type

python fetchTags.py sw -w “10m” "12:00 April.07.2019"


Using windowing option 'ew'
---------------------------
Similar to sw, except that you specify the end time and the time window. In this case
both arguments are optional. The time window defaults to 1 day and the end time 
defaults to the present time. 

To extract the time series in the example (ref 'se' case) you would type

python fetchTags.py ew -w “10m” -e "12:10 April.07.2019"


Additional Options
==================

Averaging
---------
A time series is fetched in packets. The values for the packets can be averaged 
so that only one value is written to file. So if the user is fetching a time 
series over the past 10 years and wants just daily averages, the following can be used

python fetchTags.py --pave -psize “1d” ew -w "3650d"   

where --psize specifies the packet size with format as per time window and --pave
specifies we want to average the values in the packet. 
"""

import re
import win32com.client
import sys
from dateutil.parser import parse
from datetime import datetime, timedelta
import argparse
import os

__author__ = "Tim Kendon"
__copyright__ = "Free"
__credits__ = ["No-one so far"]
__license__ = "None"
__version__ = "1.0.0"
__maintainer__ = "Tim Kendon"
__email__ = "tike@equinor.com"
__status__ = "Production"


def abodb_connect(userid="piadmin", server="ONO-IMS"):
    """Opens and returns a connection to the database (cstr = connection_string)"""
    connection = win32com.client.gencache.EnsureDispatch('ADODB.Connection')
    cstr = 'PROVIDER=PIOLEDB.1;User ID=%s;Data Source=%s; Persist Security Info=False;'
    connection.Open(cstr % (userid, server))
    return connection


def adodb_query(connection, tag, startTime, endTime):
    """ for example
      tag       = B_LRS.MRU.Pitch (as shown in say PI-Explorer)
      startTime = 06.03.2013 22:00:00
      endTime   = 06.03.2013 23:00:00
    """
    query_str = """select tag, Format(time,'dd.MM.yyyy HH:mm:ss.fff') time, cast (value as float32) as "value" from piarchive..picomp2 where (tag LIKE '%%%s%%') and time >= '%s' and time <= '%s' order by tag,time""" % (
        tag, startTime, endTime)
    rs, opt = connection.Execute(query_str)
    return rs


def export(rs, filename, action):
    """ Takes a record (rs) from an adobe_query call and exports to a file opened with 
    command action, where action should be w or a. 
    """
    with open(filename, action) as f:
        if not (rs.BOF or rs.EOF):
            rows = zip(*rs.GetRows())
            for item in rows:
                f.write('%s  %s  %.5e\n' % item)


def export_average(rs, filename, action, a_date):
    """ Takes a record (rs) from an adobe_query call, averages it, and then 
    exports it to a file opened with command action, where action should be w or a. 
    """
    with open(filename, action) as f:
        if not (rs.BOF or rs.EOF):
            rows = zip(*rs.GetRows())
            ave, num = 0, 0
            for item in rows:
                tagname, d, v = item
                ave += v
                num += 1
            ave = ave/num
            f.write('%s  %s  %.5e\n' % (tagname, a_date, ave))


def getTag(tag, startTime, endTime, id, packetsize, average, **kwargs):
    """ Gets the time series for a tag between two times and saves to a file
    with a unique prefixed identifier (id). The time series is fetched in packets 
    of duration packetsize, where packetsize is a datetime.timedelta object. 
    If you want to average the values in a packet so that only one value is saved
    per packet you can set average to True.
    """
    connection = abodb_connect(userid=kwargs["user"], server=kwargs["server"])
    delta = packetsize  # timedelta(days=30)
    filename = id + tag.replace('/', '_') + '.txt'

    # Now proceed in packets of size packetsize
    d = startTime
    while d < endTime:

        date1 = d.strftime('%d.%m.%Y  %H:%M:%S')
        d += delta
        date2 = d.strftime('%d.%m.%Y  %H:%M:%S')
        action = 'a' if os.path.isfile(filename) else 'w'

        try:
            recordSet = adodb_query(connection, tag, date1, date2)
            if average:
                export_average(recordSet, filename, action,
                               (d-delta/2).strftime('%d.%m.%Y  %H:%M:%S'))
            else:
                export(recordSet, filename, action)
        except:
            pass

    # return a flag to indicate if the file is empty or not
    isEmpty = os.stat(filename).st_size == 0
    if isEmpty:
        os.remove(filename)
    return isEmpty


def get_tags():
    """ Open tag.list file. The file contains tag names as shown in say PI-Explorer 
    """
    with open("tag.list", "r") as f:
        lines = f.readlines()
        tag_list = [line.strip() for line in lines if len(line.strip()) != 0]
    return tag_list


def extract_data(start, stop, packetsize, average, **kwargs):
    """ Extracts data from PI into files and signals if a signal was returned
    """
    tags = get_tags()
    for tag in tags:
        isEmpty = getTag(tag, start, stop, '', packetsize, average, **kwargs)
        print(start, stop, tag, "Empty" if isEmpty else "Signal")


def parse_time(time_str):
    """ a str => timedelta parser
    """
    regex = re.compile(
        r'((?P<days>\d+?)d)?((?P<hours>\d+?)hr)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?')
    parts = regex.match(time_str)
    if not parts:
        return
    parts = parts.groupdict()
    time_params = {}
    for (name, param) in parts.items():
        if param:
            time_params[name] = int(param)
    return timedelta(**time_params)


def get_cli_args():
    parser = argparse.ArgumentParser(
        description='A script for extracting time series from PI')
    parser.add_argument('-v', '--verbose', action="store_true",
                        help="verbose print")
    parser.add_argument('--user', type=str,
                        default="piadmin", help="default: piadmin")
    parser.add_argument('--server', type=str,
                        default="ONO-IMS", help="default: ONO-IMS")
    parser.add_argument('--pave', action="store_true",
                        help="to average the packet values")
    parser.add_argument('--taglist', type=str, default="tag.list",
                        help="file containing tag names (default tag.list)")
    parser.add_argument(
        '--psize', type=lambda s: parse_time(s), default=timedelta(days=1),
        help="default: fetches data in 1 day chunks")
    subparsers = parser.add_subparsers(
        dest="subparser_name", help='windowing options')
    a_subparser = subparsers.add_parser("se")
    a_subparser.add_argument(
        's', type=lambda s: parse(s, fuzzy=True), help="start datetime")
    a_subparser.add_argument(
        'e', type=lambda s: parse(s, fuzzy=True), help="end datetime")
    a_subparser = subparsers.add_parser("ew")
    a_subparser.add_argument(
        "-e", type=lambda s: parse(s, fuzzy=True), default=datetime.now(),
        help="end datetime")
    a_subparser.add_argument(
        "-w", type=lambda s: parse_time(s), default=timedelta(days=1),
        help="time window")
    a_subparser = subparsers.add_parser("sw")
    a_subparser.add_argument(
        "s", type=lambda s: parse(s, fuzzy=True), help="start datetime")
    a_subparser.add_argument(
        "-w", type=lambda s: parse_time(s), default=timedelta(days=1), help="time window")

    if len(sys.argv[1:]) == 0:
        parser.print_help(sys.stderr)
        # parser.print_usage() # for just the usage line
        parser.exit()

    return parser.parse_args(), parser


if __name__ == '__main__':

    # Return parser as well for help
    args, parser = get_cli_args()

    if args.subparser_name == "se":
        start, end = args.s, args.e
    elif args.subparser_name == "sw":
        start, end = args.s, args.s + args.w
    elif args.subparser_name == "ew":
        start, end = args.e - args.w, args.e

    if start == None or end == None:
        print("Error with CLI")
        print("Start time registered as ", start)
        print("End time registered as ", start)
        parser.print_help(sys.stderr)
        sys.exit()

    server = args.server
    user = args.user
    average = args.pave
    packet_size = args.psize

    if end < start:
        print("End time cannot be after start time!")
        if args.verbose:
            print("End time", end)
            print("Start time", start)
        sys.exit()

    if end-start < packet_size:
        packet_size = end-start
        if args.verbose:
            print("Packet_size being reduced bigger than requested time window")
            print("Packet size being reduced accordingly")

    extract_data(start, end, packet_size, average,
                 server=server, user=user)
