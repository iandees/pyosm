import pyosm.model
from pyosm.parsing import iter_osm_stream
import pyes
import sys

es = pyes.ES(['ec2-54-226-182-114.compute-1.amazonaws.com:9200'], timeout=3, max_retries=3)

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
            'type': 'geo_point',
            'lat_lon': True
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

try:
    es.indices.delete_index('osm-archive')
except:
    pass
es.indices.create_index('osm-archive')
es.indices.put_mapping('node', node_mapping, ['osm-archive'])
es.indices.put_mapping('way', way_mapping, ['osm-archive'])
es.indices.put_mapping('relation', relation_mapping, ['osm-archive'])

n = 0
nodes = 0
ways = 0
relations = 0
sys.stdout.write('%10d nodes, %8d ways, %5d relations' % (nodes, ways, relations))
for (verb, p) in iter_osm_stream():
    data = {
        'id': p.id,
        'version': p.version,
        'changeset': p.changeset,
        'timestamp': p.timestamp,
        'user': p.user,
        'uid': p.uid
    }

    data['tags'] = dict([(tag.key, tag.value) for tag in p.tags])

    if type(p) == pyosm.model.Node:
        data['loc'] = {'lat': p.lat, 'lon': p.lon}
        es.index(data, 'osm-archive', 'node', '%s.%s' % (p.id, p.version), bulk=True)
        nodes += 1
    elif type(p) == pyosm.model.Way:
        data['nds'] = p.nds
        es.index(data, 'osm-archive', 'way', '%s.%s' % (p.id, p.version), bulk=True)
        ways += 1
    elif type(p) == pyosm.model.Relation:
        data['members'] = [dict(type=r.type, ref=r.ref, role=r.role) for r in p.members]
        es.index(data, 'osm-archive', 'relation', '%s.%s' % (p.id, p.version), bulk=True)
        relations += 1
    n += 1

    #sys.stdout.write('\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b')
    #sys.stdout.write('%10d nodes, %8d ways, %5d relations' % (nodes, ways, relations))
    #sys.stdout.flush()

es.flush_bulk()

#sys.stdout.write('\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b')
#sys.stdout.write('%10d nodes, %8d ways, %5d relations\n' % (nodes, ways, relations))
#sys.stdout.flush()
