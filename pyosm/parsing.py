import datetime
import gzip
import io
import os.path
import pyosm.model as model
import requests
import time
from lxml import etree


def isoToDatetime(s):
    """Parse a ISO8601-formatted string to a Python datetime."""
    if s is None:
        return s
    else:
        return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")


def noteTimeToDatetime(s):
    """Parse a datetime out of the Notes RSS feed."""
    if s is None:
        return s
    else:
        # 2013-05-01 08:10:42 UTC
        return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S UTC")


def maybeInt(s):
    return int(s) if s is not None else s


def maybeFloat(s):
    return float(s) if s is not None else s


def maybeBool(s):
    return s == 'true' if s is not None else s


def readState(state_file, sep='='):
    state = {}

    for line in state_file.splitlines():
        if line.startswith('---'):
            continue
        if line[0] == '#':
            continue
        (k, v) = line.split(sep)
        state[k] = v.strip().replace("\\:", ":")

    return state


def iter_changeset_stream(start_sqn=None, base_url='https://planet.openstreetmap.org/replication/changesets', expected_interval=60, parse_timestamps=True, state_dir=None):
    """Start processing an OSM changeset stream and yield one (action, primitive) tuple
    at a time to the caller."""

    # This is a lot like the other osm_stream except there's no
    # state file for each of the diffs, so just push ahead until
    # we run into a 404.

    # If the user specifies a state_dir, read the state from the statefile there
    if state_dir:
        if not os.path.exists(state_dir):
            raise Exception('Specified state_dir "%s" doesn\'t exist.' % state_dir)

        if os.path.exists('%s/state.yaml' % state_dir):
            with open('%s/state.yaml' % state_dir, 'r') as f:
                state = readState(f, ': ')
                start_sqn = state['sequence']

    # If no start_sqn, assume to start from the most recent changeset file
    if not start_sqn:
        u = requests.get('%s/state.yaml' % base_url)
        u.raise_for_status()
        state = readState(u.text, ': ')
        sequenceNumber = int(state['sequence'])
    else:
        sequenceNumber = int(start_sqn)

    interval_fudge = 0.0
    while True:
        sqnStr = str(sequenceNumber).zfill(9)
        url = '%s/%s/%s/%s.osm.gz' % (base_url, sqnStr[0:3], sqnStr[3:6], sqnStr[6:9])

        delay = 1.0
        while True:
            content = requests.get(url)

            if content.status_code == 404:
                time.sleep(delay)
                delay = min(delay * 2, 13)
                interval_fudge += delay
                continue

            content = io.BytesIO(content.content)
            gzipper = gzip.GzipFile(fileobj=content)
            interval_fudge -= (interval_fudge / 2.0)
            break

        obj = None
        for event, elem in etree.iterparse(gzipper, events=('start', 'end')):
            if event == 'start':
                if elem.tag == 'changeset':
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
                elif elem.tag == 'tag':
                    obj.tags.append(
                        model.Tag(
                            elem.attrib['k'],
                            elem.attrib['v']
                        )
                    )
            elif event == 'end':
                if elem.tag == 'changeset':
                    yield obj
                    obj = None

        yield model.Finished(sequenceNumber, None)

        sequenceNumber += 1

        if state_dir:
            with open('%s/state.yaml' % state_dir, 'w') as f:
                f.write('sequence: %d' % sequenceNumber)


def iter_osm_change_file(f, parse_timestamps=True):
    action = None
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
                    maybeFloat(elem.attrib.get('lat')),
                    maybeFloat(elem.attrib.get('lon')),
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

        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]


def iter_osm_stream(start_sqn=None, base_url='https://planet.openstreetmap.org/replication/minute', expected_interval=60, parse_timestamps=True, state_dir=None):
    """Start processing an OSM diff stream and yield one changeset at a time to
    the caller."""

    # If the user specifies a state_dir, read the state from the statefile there
    if state_dir:
        if not os.path.exists(state_dir):
            raise Exception('Specified state_dir "%s" doesn\'t exist.' % state_dir)

        if os.path.exists('%s/state.txt' % state_dir):
            with open('%s/state.txt' % state_dir) as f:
                state = readState(f)
                start_sqn = state['sequenceNumber']

    # If no start_sqn, assume to start from the most recent diff
    if not start_sqn:
        u = requests.get('%s/state.txt' % base_url)
        state = readState(u.text)
    else:
        sqnStr = str(start_sqn).zfill(9)
        u = requests.get('%s/%s/%s/%s.state.txt' % (base_url, sqnStr[0:3], sqnStr[3:6], sqnStr[6:9]))
        state = readState(u.text)

    interval_fudge = 0.0

    while True:
        sqnStr = state['sequenceNumber'].zfill(9)
        url = '%s/%s/%s/%s.osc.gz' % (base_url, sqnStr[0:3], sqnStr[3:6], sqnStr[6:9])
        content = requests.get(url)
        content = io.BytesIO(content.content)
        gzipper = gzip.GzipFile(fileobj=content)

        for a in iter_osm_change_file(gzipper, parse_timestamps):
            yield a

        # After parsing the OSC, check to see how much time is remaining
        stateTs = datetime.datetime.strptime(state['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
        yield (None, model.Finished(state['sequenceNumber'], stateTs))

        nextTs = stateTs + datetime.timedelta(seconds=expected_interval + interval_fudge)
        if datetime.datetime.utcnow() < nextTs:
            timeToSleep = (nextTs - datetime.datetime.utcnow()).total_seconds()
        else:
            timeToSleep = 0.0
        time.sleep(timeToSleep)

        # Then try to fetch the next state file
        sqnStr = str(int(state['sequenceNumber']) + 1).zfill(9)
        url = '%s/%s/%s/%s.state.txt' % (base_url, sqnStr[0:3], sqnStr[3:6], sqnStr[6:9])
        delay = 1.0
        while True:
            u = requests.get(url)

            if u.status_code == 404:
                time.sleep(delay)
                delay = min(delay * 2, 13)
                interval_fudge += delay
                continue

            interval_fudge -= (interval_fudge / 2.0)
            break

        if state_dir:
            with open('%s/state.txt' % state_dir, 'w') as f:
                f.write(u.text)
            with open('%s/state.txt' % state_dir, 'r') as f:
                state = readState(f)
        else:
            state = readState(u.text)


def iter_osm_file(f, parse_timestamps=True):
    """Parse a file-like containing OSM XML and yield one OSM primitive at a time
    to the caller."""

    obj = None
    for event, elem in etree.iterparse(f, events=('start', 'end')):
        if event == 'start':
            if elem.tag == 'node':
                obj = model.Node(
                    int(elem.attrib['id']),
                    maybeInt(elem.get('version')),
                    maybeInt(elem.get('changeset')),
                    elem.attrib.get('user'),
                    maybeInt(elem.attrib.get('uid')),
                    maybeBool(elem.attrib.get('visible')),
                    isoToDatetime(elem.attrib.get('timestamp')) if parse_timestamps else elem.attrib.get('timestamp'),
                    maybeFloat(elem.get('lat')),
                    maybeFloat(elem.get('lon')),
                    []
                )
            elif elem.tag == 'way':
                obj = model.Way(
                    int(elem.attrib['id']),
                    maybeInt(elem.get('version')),
                    maybeInt(elem.get('changeset')),
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
                    maybeInt(elem.get('version')),
                    maybeInt(elem.get('changeset')),
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

        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]


def parse_osm_file(f, parse_timestamps=True):
    """Parse a file-like containing OSM XML into memory and return an object with
    the nodes, ways, and relations it contains. """

    nodes = []
    ways = []
    relations = []

    for p in iter_osm_file(f, parse_timestamps):

        if type(p) == model.Node:
            nodes.append(p)
        elif type(p) == model.Way:
            ways.append(p)
        elif type(p) == model.Relation:
            relations.append(p)

    return (nodes, ways, relations)


def get_note(note_id, parse_timestamps=True):
    u = requests.get('https://www.openstreetmap.org/api/0.6/notes/%d' % note_id)
    u.raise_for_status()
    tree = etree.fromstring(u.content)
    note_elem = tree.xpath('/osm/note')[0]

    def parse_comment(comment_element):
        user_elem = comment_element.xpath('user')
        uid_elem = comment_element.xpath('uid')
        return model.Comment(
            created_at=noteTimeToDatetime(comment_element.xpath('date')[0].text) if parse_timestamps else comment_element.xpath('date')[0].text,
            user=user_elem[0].text if user_elem else None,
            uid=int(uid_elem[0].text) if uid_elem else None,
            action=comment_element.xpath('action')[0].text,
            text=comment_element.xpath('text')[0].text
        )

    closed_elem = note_elem.xpath('date_closed')
    if closed_elem:
        closed_at = noteTimeToDatetime(closed_elem[0].text) if parse_timestamps else closed_elem[0].text
    else:
        closed_at = None

    return model.Note(
        id=int(note_elem.xpath('id')[0].text),
        lat=float(note_elem.attrib['lat']),
        lon=float(note_elem.attrib['lon']),
        created_at=noteTimeToDatetime(note_elem.xpath('date_created')[0].text) if parse_timestamps else note_elem.xpath('date_created')[0].text,
        closed_at=closed_at,
        status=note_elem.xpath('status')[0].text,
        comments=[parse_comment(c) for c in note_elem.xpath('comments/comment')]
    )


def iter_osm_notes(feed_limit=25, interval=60, parse_timestamps=True):
    """ Parses the global OSM Notes feed and yields as much Note information as possible. """

    last_seen_guid = None
    while True:
        u = requests.get(
            'https://www.openstreetmap.org/api/0.6/notes/feed',
            params=dict(limit=feed_limit),
        )
        u.raise_for_status()

        tree = etree.fromstring(u.content)

        new_notes = []
        for note_item in tree.xpath('/rss/channel/item'):
            title = note_item.xpath('title')[0].text

            if title.startswith('new note ('):
                action = 'create'
            elif title.startswith('new comment ('):
                action = 'comment'
            elif title.startswith('closed note ('):
                action = 'close'

            # Note that (at least for now) the link and guid are the same in the feed.
            guid = note_item.xpath('link')[0].text

            if last_seen_guid == guid:
                break
            elif last_seen_guid is None:
                # The first time through we want the first item to be the "last seen"
                # because the RSS feed is newest-to-oldest
                last_seen_guid = guid
            else:
                note_id = int(guid.split('/')[-1].split('#c')[0])
                new_notes.append((action, get_note(note_id, parse_timestamps)))

        # We yield the reversed list because we want to yield in change order
        # (i.e. "oldest to most current")
        for note in reversed(new_notes):
            yield note

        yield model.Finished(None, None)

        time.sleep(interval)
