import model
import datetime
import urllib2
import StringIO
import gzip
import time
from lxml import etree

def isoToDatetime(s):
    """Parse a ISO8601-formatted string to a Python datetime."""
    if s is None:
        return s
    else:
        return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")

def maybeInt(s):
    return int(s) if s is not None else s

def maybeFloat(s):
    return float(s) if s is not None else s

def maybeBool(s):
    if s is not None:
        if s == 'true':
            return True
        else:
            return False
    else:
        return s

def readState(state_file):
    state = {}

    for line in state_file:
        if line[0] == '#':
            continue
        (k, v) = line.split('=')
        state[k] = v.strip().replace("\\:", ":")

    return state

def iter_osm_stream(start_sqn=None, base_url='http://planet.openstreetmap.org/replication/minute', expected_interval=60, parse_timestamps=True):
    """Start processing an OSM diff stream and yield one (action, primitive) tuple
    at a time to the caller."""

    # If no start_sqn, assume to start from the most recent diff
    if not start_sqn:
        u = urllib2.urlopen('%s/state.txt' % base_url)
        state = readState(u)
    else:
        sqnStr = str(start_sqn).zfill(9)
        u = urllib2.urlopen('%s/%s/%s/%s.state.txt' % (base_url, sqnStr[0:3], sqnStr[3:6], sqnStr[6:9]))
        state = readState(u)

    interval_fudge = 0.0

    while True:
        sqnStr = state['sequenceNumber'].zfill(9)
        url = '%s/%s/%s/%s.osc.gz' % (base_url, sqnStr[0:3], sqnStr[3:6], sqnStr[6:9])
        print "Fetching %s" % url
        content = urllib2.urlopen(url)
        content = StringIO.StringIO(content.read())
        gzipper = gzip.GzipFile(fileobj=content)

        action = None
        obj = None
        for event, elem in etree.iterparse(gzipper, events=('start', 'end')):
            if event == 'start':
                if elem.tag == 'node':
                    obj = model.Node(
                        int(elem.attrib['id']),
                        int(elem.attrib['version']),
                        int(elem.attrib['changeset']),
                        elem.attrib.get('user'),
                        maybeInt(elem.attrib.get('uid')),
                        maybeBool(elem.attrib.get('visible')),
                        isoToDatetime(elem.attrib.get('timestamp')) if parse_timestamps else elem.attrib.get('timestamp'),
                        float(elem.attrib['lat']),
                        float(elem.attrib['lon']),
                        []
                    )
                elif elem.tag == 'way':
                    obj = model.Way(
                        int(elem.attrib['id']),
                        int(elem.attrib['version']),
                        int(elem.attrib['changeset']),
                        elem.attrib.get('user'),
                        maybeInt(elem.attrib.get('uid')),
                        maybeBool(elem.attrib.get('visible')),
                        isoToDatetime(elem.attrib.get('timestamp')) if parse_timestamps else elem.attrib.get('timestamp'),
                        [],
                        []
                    )
                elif elem.tag == 'tag':
                    obj.tags.append(
                        model.Tag(
                            elem.attrib['k'],
                            elem.attrib['v']
                        )
                    )
                elif elem.tag == 'nd':
                    obj.nds.append(int(elem.attrib['ref']))
                elif elem.tag == 'relation':
                    obj = model.Relation(
                        int(elem.attrib['id']),
                        int(elem.attrib['version']),
                        int(elem.attrib['changeset']),
                        elem.attrib.get('user'),
                        maybeInt(elem.attrib.get('uid')),
                        maybeBool(elem.attrib.get('visible')),
                        isoToDatetime(elem.attrib.get('timestamp')) if parse_timestamps else elem.attrib.get('timestamp'),
                        [],
                        []
                    )
                elif elem.tag == 'member':
                    obj.members.append(
                        model.Member(
                            elem.attrib['type'],
                            int(elem.attrib['ref']),
                            elem.attrib['role']
                        )
                    )
                elif elem.tag in ('create', 'modify', 'delete'):
                    action = elem.tag
            elif event == 'end':
                if elem.tag == 'node':
                    yield (action, obj)
                    obj = None
                elif elem.tag == 'way':
                    yield (action, obj)
                    obj = None
                elif elem.tag == 'relation':
                    yield (action, obj)
                    obj = None
                elif elem.tag in ('create', 'modify', 'delete'):
                    action = None

        # After parsing the OSC, check to see how much time is remaining
        stateTs = datetime.datetime.strptime(state['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
        nextTs = stateTs + datetime.timedelta(seconds=expected_interval + interval_fudge)
        if datetime.datetime.utcnow() < nextTs:
            timeToSleep = (nextTs - datetime.datetime.utcnow()).total_seconds()
            print "Waiting %s (%0.2f fudge) seconds for the next diff" % (timeToSleep, interval_fudge)
        else:
            timeToSleep = 0.0
        time.sleep(timeToSleep)

        # Then try to fetch the next state file
        sqnStr = str(int(state['sequenceNumber']) + 1).zfill(9)
        url = '%s/%s/%s/%s.state.txt' % (base_url, sqnStr[0:3], sqnStr[3:6], sqnStr[6:9])
        delay = 1.0
        while True:
            print 'Fetching %s' % url
            try:
                u = urllib2.urlopen(url)
                interval_fudge -= (interval_fudge / 2.0)
                break
            except urllib2.HTTPError, e:
                if e.code == 404:
                    print "%s doesn't exist yet. Waiting %.1f seconds." % (url, delay)
                    time.sleep(delay)
                    delay = min(delay * 2, 13)
                    interval_fudge += delay

        state = readState(u)

def iter_osm_file(f, parse_timestamps=True):
    """Parse a file-like containing OSM XML and yield one OSM primitive at a time
    to the caller."""

    obj = None
    for event, elem in etree.iterparse(f, events=('start', 'end')):
        if event == 'start':
            if elem.tag == 'node':
                obj = model.Node(
                    int(elem.attrib['id']),
                    int(elem.attrib['version']),
                    int(elem.attrib['changeset']),
                    elem.attrib.get('user'),
                    maybeInt(elem.attrib.get('uid')),
                    maybeBool(elem.attrib.get('visible')),
                    isoToDatetime(elem.attrib.get('timestamp')) if parse_timestamps else elem.attrib.get('timestamp'),
                    float(elem.attrib['lat']),
                    float(elem.attrib['lon']),
                    []
                )
            elif elem.tag == 'way':
                obj = model.Way(
                    int(elem.attrib['id']),
                    int(elem.attrib['version']),
                    int(elem.attrib['changeset']),
                    elem.attrib.get('user'),
                    maybeInt(elem.attrib.get('uid')),
                    maybeBool(elem.attrib.get('visible')),
                    isoToDatetime(elem.attrib.get('timestamp')) if parse_timestamps else elem.attrib.get('timestamp'),
                    [],
                    []
                )
            elif elem.tag == 'tag':
                obj.tags.append(
                    model.Tag(
                        elem.attrib['k'],
                        elem.attrib['v']
                    )
                )
            elif elem.tag == 'nd':
                obj.nds.append(int(elem.attrib['ref']))
            elif elem.tag == 'relation':
                obj = model.Relation(
                    int(elem.attrib['id']),
                    int(elem.attrib['version']),
                    int(elem.attrib['changeset']),
                    elem.attrib.get('user'),
                    maybeInt(elem.attrib.get('uid')),
                    maybeBool(elem.attrib.get('visible')),
                    isoToDatetime(elem.attrib.get('timestamp')) if parse_timestamps else elem.attrib.get('timestamp'),
                    [],
                    []
                )
            elif elem.tag == 'member':
                obj.members.append(
                    model.Member(
                        elem.attrib['type'],
                        int(elem.attrib['ref']),
                        elem.attrib['role']
                    )
                )
            elif elem.tag == 'changeset':
                obj = model.Changeset(
                    int(elem.attrib['id']),
                    isoToDatetime(elem.attrib.get('created_at')) if parse_timestamps else elem.attrib.get('created_at'),
                    isoToDatetime(elem.attrib.get('closed_at')) if parse_timestamps else elem.attrib.get('closed_at'),
                    maybeBool(elem.attrib['open']),
                    maybeFloat(elem.get('min_lat')),
                    maybeFloat(elem.get('max_lat')),
                    maybeFloat(elem.get('min_lon')),
                    maybeFloat(elem.get('max_lon')),
                    elem.attrib.get('user'),
                    maybeInt(elem.attrib.get('uid')),
                    []
                )
        elif event == 'end':
            if elem.tag == 'node':
                yield obj
                obj = None
            elif elem.tag == 'way':
                yield obj
                obj = None
            elif elem.tag == 'relation':
                yield obj
                obj = None
            elif elem.tag == 'changeset':
                yield obj
                obj = None
