import pyosm.model
from pyosm.parsing import iter_osm_file
import pyes
import sys

es = pyes.ES(['ec2-54-226-182-114.compute-1.amazonaws.com:9200'], timeout=60, bulk_size=100)


changeset_mapping = {
    '_timestamp': {
        'enabled': True,
        'path': 'created_at'
    },
    'properties': {
        'id': {
            'type': 'long'
        },
        'created_at': {
            'type': 'date'
        },
        'closed_at': {
            'type': 'date'
        },
        'open': {
            'type': 'boolean'
        },
        'user': {
            'type': 'string',
            'index': 'not_analyzed'
        },
        'uid': {
            'type': 'integer'
        },
        'bbox': {
            'type': 'geo_shape',
            'tree': 'geohash',
            'precision': '1000m'
        },
        'tags': {
            'type': 'object',
            'enabled': False
        }
    }
}
node_mapping = {
    '_timestamp': {
        'enabled': True,
        'path': 'timestamp'
    },
    'properties': {
        'id': {
            'type': 'long'
        },
        'version': {
            'type': 'integer'
        },
        'changeset': {
            'type': 'integer'
        },
        'timestamp': {
            'type': 'date'
        },
        'user': {
            'type': 'string',
            'index': 'not_analyzed'
        },
        'uid': {
            'type': 'integer'
        },
        'tags': {
            'type': 'object',
            'enabled': False
        },
        'loc': {
            'type': 'geo_shape',
            'precision': '10m'
        }
    }
}
way_mapping = {
    '_timestamp': {
        'enabled': True,
        'path': 'timestamp'
    },
    'properties': {
        'id': {
            'type': 'long'
        },
        'version': {
            'type': 'integer'
        },
        'changeset': {
            'type': 'integer'
        },
        'timestamp': {
            'type': 'date'
        },
        'user': {
            'type': 'string',
            'index': 'not_analyzed'
        },
        'uid': {
            'type': 'integer'
        },
        'tags': {
            'type': 'object',
            'enabled': False
        }
    }
}
relation_mapping = {
    '_timestamp': {
        'enabled': True,
        'path': 'timestamp'
    },
    'properties': {
        'id': {
            'type': 'long'
        },
        'version': {
            'type': 'integer'
        },
        'changeset': {
            'type': 'integer'
        },
        'timestamp': {
            'type': 'date'
        },
        'user': {
            'type': 'string',
            'index': 'not_analyzed'
        },
        'uid': {
            'type': 'integer'
        },
        'tags': {
            'type': 'object',
            'enabled': False
        }
    }
}

# try:
#     es.indices.delete_index('osm-archive')
# except:
#     pass
# es.indices.create_index('osm-archive')
# es.indices.put_mapping('node', node_mapping, ['osm-archive'])
# es.indices.put_mapping('way', way_mapping, ['osm-archive'])
# es.indices.put_mapping('relation', relation_mapping, ['osm-archive'])
# es.indices.put_mapping('changeset', changeset_mapping, ['osm-archive'])

n = 0
nodes = 0
ways = 0
relations = 0
changesets = 0
sys.stdout.write('%8d changesets, %10d nodes, %8d ways, %5d relations' % (changesets, nodes, ways, relations))
for p in iter_osm_file(open(sys.argv[1], 'r'), parse_timestamps=False):

    if type(p) == pyosm.model.Node:
        data = {
            'id': p.id,
            'version': p.version,
            'changeset': p.changeset,
            'timestamp': p.timestamp,
            'user': p.user,
            'uid': p.uid,
            'loc': {'type': 'point', 'coordinates': [p.lon, p.lat]},
            'tags': dict([(tag.key, tag.value) for tag in p.tags])
        }
        es.index(data, 'osm-archive', 'node', '%s.%s' % (p.id, p.version), bulk=True)
        nodes += 1
    elif type(p) == pyosm.model.Way:
        data = {
            'id': p.id,
            'version': p.version,
            'changeset': p.changeset,
            'timestamp': p.timestamp,
            'user': p.user,
            'uid': p.uid,
            'tags': dict([(tag.key, tag.value) for tag in p.tags]),
            'nds': p.nds
        }
        es.index(data, 'osm-archive', 'way', '%s.%s' % (p.id, p.version), bulk=True)
        ways += 1
    elif type(p) == pyosm.model.Relation:
        data = {
            'id': p.id,
            'version': p.version,
            'changeset': p.changeset,
            'timestamp': p.timestamp,
            'user': p.user,
            'uid': p.uid,
            'tags': dict([(tag.key, tag.value) for tag in p.tags]),
            'members': [dict(type=r.type, ref=r.ref, role=r.role) for r in p.members]
        }
        es.index(data, 'osm-archive', 'relation', '%s.%s' % (p.id, p.version), bulk=True)
        relations += 1
    elif type(p) == pyosm.model.Changeset:
        data = {
            'id': p.id,
            'created_at': p.created_at,
            'closed_at': p.closed_at,
            'user': p.user,
            'uid': p.uid,
            'open': p.open,
            'tags': dict([(tag.key, tag.value) for tag in p.tags])
        }
        if p.min_lon:
            data['bbox'] = {'type': 'envelope', 'coordinates': [[p.min_lon, p.max_lat], [p.max_lon, p.min_lat]]}

        es.index(data, 'osm-archive', 'changeset', p.id, bulk=True)
        changesets += 1
    n += 1

    sys.stdout.write('\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b')
    sys.stdout.write('%8d changesets, %10d nodes, %8d ways, %5d relations' % (changesets, nodes, ways, relations))
    sys.stdout.flush()

es.refresh()

sys.stdout.write('\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b')
sys.stdout.write('%10d nodes, %8d ways, %5d relations\n' % (nodes, ways, relations))
sys.stdout.write('%8d changesets, %10d nodes, %8d ways, %5d relations' % (changesets, nodes, ways, relations))
sys.stdout.flush()
