import urllib2
from parsing import iter_osm_file, iter_osm_change_file

class Api(object):
    def __init__(self, base_url='http://api.openstreetmap.org/api'):
        self._base = base_url
        self.USER_AGENT = 'pyosm/1.0 (http://github.com/iandees/pyosm)'

    def _get(self, path, params={}):
        headers = {
            'User-Agent': self.USER_AGENT
        }
        req = urllib2.Request(self._base + path, headers=headers)

        return urllib2.urlopen(req)

    def _get_as_osm(self, path, params={}):
        return [t for t in iter_osm_file(self._get(path, params))]

    def _get_object_revision_as_osm(self, kind, thing_id, version=None):
        path = '/0.6/{}/{}'.format(kind, thing_id)
        if version:
            path += '/' + str(version)

        everything = self._get_as_osm(path)

        single = None
        if everything:
            single = next(iter(everything))

        return single

    def get_node(self, node_id, version=None):
        return self._get_object_revision_as_osm('node', node_id, version)

    def get_way(self, way_id, version=None):
        return self._get_object_revision_as_osm('way', way_id, version)

    def get_relation(self, relation_id, version=None):
        return self._get_object_revision_as_osm('relation', relation_id, version)

    def _get_objects_as_osm(self, kind, thing_ids):
        plural_kind = kind + 's'
        path = '/0.6/{}'.format(plural_kind)

        everything = self._get_as_osm(path, params={plural_kind: thing_ids})

        return everything

    def get_nodes(self, node_ids):
        return self._get_objects_as_osm('node', node_ids)

    def get_ways(self, way_ids):
        return self._get_objects_as_osm('way', way_ids)

    def get_relations(self, relation_ids):
        return self._get_objects_as_osm('relation', relation_ids)

    def _get_object_history_as_osm(self, kind, thing_id):
        path = '/0.6/{}/{}/history'.format(kind, thing_id)

        everything = self._get_as_osm(path)

        return everything

    def get_node_history(self, node_id):
        return self._get_object_history_as_osm('node', node_id)

    def get_way_history(self, way_id):
        return self._get_object_history_as_osm('way', way_id)

    def get_relation_history(self, relation_id):
        return self._get_object_history_as_osm('relation', relation_id)

    def get_changeset_download(self, changeset_id):
        return [t for t in iter_osm_change_file(self._get('/0.6/changeset/{}/download'.format(changeset_id)))]

    def get_changeset_metadata(self, changeset_id):
        return next(iter(self._get_as_osm('/0.6/changeset/{}'.format(changeset_id))))
