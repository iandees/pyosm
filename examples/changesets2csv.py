from pyosm.parsing import iter_changeset_stream
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

size_of_buffer = 100
size_of_slice = 1000000

def cut_new_file(kind):
    global gzips, counter, csvs

    old_gz = getattr(gzips, kind)
    if old_gz:
        old_gz.close()

    kind_gz = gzip.GzipFile('%s.viadiff.csv.%05d.gz' % (kind, getattr(counter, kind) / size_of_slice), 'w')
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

sys.stdout.write('%8d changesets, %10d nodes, %10d ways, %10d relations' % (counter.changesets, counter.nodes, counter.ways, counter.relations))
for changeset in iter_changeset_stream(start_sqn=141042, parse_timestamps=False):

    buffers.changesets.append([
        changeset.id,
        changeset.created_at,
        changeset.closed_at,
        changeset.user,
        changeset.uid,
        ','.join(['"%s"=>"%s"' % (re.escape(tag.key), re.escape(tag.value)) for tag in changeset.tags]),
        '%0.7f, %0.7f, %0.7f, %0.7f' % (changeset.min_lon, changeset.max_lat, changeset.max_lon, changeset.min_lat) if changeset.min_lon else None
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

sys.stdout.write('\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b')
sys.stdout.write('%8d changesets, %10d nodes, %10d ways, %10d relations' % (counter.changesets, counter.nodes, counter.ways, counter.relations))
sys.stdout.flush()
