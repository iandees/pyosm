import model
import datetime
from lxml import etree

def isoToDatetime(s):
    """Parse a ISO8601-formatted string to a Python datetime."""
    return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")

def maybeInt(s):
    return int(s) if s is not None else s

def maybeBool(s):
    if s is not None:
        if s == 'true':
            return True
        else:
            return False
    else:
        return s

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
