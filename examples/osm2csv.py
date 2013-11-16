import pyosm.model
from pyosm.parsing import iter_osm_file
import sys
import unicodecsv
import gzip
import re

class ThingHolder(object):
    def __init__(self, nodes, ways, relations, changesets):
        self.nodes = nodes
        self.ways = ways
        self.relations = relations
        self.changesets = changesets

total = 0

counter = ThingHolder(0,0,0,0)
gzips = ThingHolder(None,None,None,None)
csvs = ThingHolder(None,None,None,None)
buffers = ThingHolder([],[],[],[])
headers = {
    'changesets': ['id', 'created_at', 'closed_at', 'user', 'uid', 'tags', 'bbox'],
    'nodes': ['id', 'version', 'changeset', 'user', 'uid', 'visible', 'timestamp', 'tags', 'loc'],
    'ways': ['id', 'version', 'changeset', 'user', 'uid', 'visible', 'timestamp', 'tags', 'nds'],
    'relations': ['id', 'version', 'changeset', 'user', 'uid', 'visible', 'timestamp', 'tags', 'members']
}

size_of_buffer = 1000
size_of_slice = 1000000

def cut_new_file(kind):
    global gzips, counter, csvs

    old_gz = getattr(gzips, kind)
    if old_gz:
        old_gz.close()

    kind_gz = gzip.GzipFile('%s.csv.%05d.gz' % (kind, getattr(counter, kind) / size_of_slice), 'w')
    setattr(gzips, kind, kind_gz)

    kind_csv = unicodecsv.writer(kind_gz)
    setattr(csvs, kind, kind_csv)

    kind_csv.writerow(headers[kind])

def write_and_clear_buffer(kind):
    global csvs, buffers, counter

    kind_buffer = getattr(buffers, kind)
    getattr(csvs, kind).writerows(kind_buffer)
    setattr(buffers, kind, [])

cut_new_file('changesets')
cut_new_file('nodes')
cut_new_file('ways')
cut_new_file('relations')

sys.stdout.write('%8d changesets, %10d nodes, %10d ways, %10d relations' % (counter.changesets, counter.nodes, counter.ways, counter.relations))
for p in iter_osm_file(open(sys.argv[1], 'r'), parse_timestamps=False):

    if type(p) == pyosm.model.Node:
        buffers.nodes.append([
            p.id,
            p.version,
            p.changeset,
            p.user,
            p.uid,
            p.visible,
            p.timestamp,
            ','.join(['"%s"=>"%s"' % (re.escape(tag.key), re.escape(tag.value)) for tag in p.tags]),
            '%0.7f, %0.7f' % (p.lon, p.lat) if p.lat else None
        ])
        counter.nodes += 1

        if counter.nodes % size_of_buffer == 0:
            write_and_clear_buffer('nodes')

        if counter.nodes % size_of_slice == 0:
            cut_new_file('nodes')

    elif type(p) == pyosm.model.Way:
        buffers.ways.append([
            p.id,
            p.version,
            p.changeset,
            p.timestamp,
            p.user,
            p.uid,
            p.visible,
            ','.join(['"%s"=>"%s"' % (re.escape(tag.key), re.escape(tag.value)) for tag in p.tags]),
            '{' + ','.join(p.nds) + '}'
        ])
        counter.ways += 1

        if counter.ways % size_of_buffer == 0:
            write_and_clear_buffer('ways')

        if counter.ways % size_of_slice == 0:
            cut_new_file('ways')

    elif type(p) == pyosm.model.Relation:
        buffers.relations.append([
            p.id,
            p.version,
            p.changeset,
            p.timestamp,
            p.user,
            p.uid,
            p.visible,
            ','.join(['"%s"=>"%s"' % (re.escape(tag.key), re.escape(tag.value)) for tag in p.tags]),
            '{' + ','.join(['{"%s","%s","%s"}' % (r.type, r.ref, r.role) for r in p.members]) + '}'
        ])
        counter.relations += 1

        if counter.relations % size_of_buffer == 0:
            write_and_clear_buffer('relations')

        if counter.relations % size_of_slice == 0:
            cut_new_file('relations')

    elif type(p) == pyosm.model.Changeset:
        buffers.changesets.append([
            p.id,
            p.created_at,
            p.closed_at,
            p.user,
            p.uid,
            ','.join(['"%s"=>"%s"' % (re.escape(tag.key), re.escape(tag.value)) for tag in p.tags]),
            '%0.7f, %0.7f, %0.7f, %0.7f' % (p.min_lon, p.max_lat, p.max_lon, p.min_lat) if p.min_lon else None
        ])

        counter.changesets += 1

        if counter.changesets % size_of_buffer == 0:
            write_and_clear_buffer('changesets')

        if counter.changesets % size_of_slice == 0:
            cut_new_file('changesets')

    total += 1

    if total % size_of_buffer == 0:
        sys.stdout.write('\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b')
        sys.stdout.write('%8d changesets, %10d nodes, %10d ways, %10d relations' % (counter.changesets, counter.nodes, counter.ways, counter.relations))
        sys.stdout.flush()

write_and_clear_buffer('changesets')
write_and_clear_buffer('nodes')
write_and_clear_buffer('ways')
write_and_clear_buffer('relations')

sys.stdout.write('\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b')
sys.stdout.write('%8d changesets, %10d nodes, %10d ways, %10d relations' % (counter.changesets, counter.nodes, counter.ways, counter.relations))
sys.stdout.flush()
