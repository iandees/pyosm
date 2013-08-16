import collections

Tag = collections.namedtuple('Tag', 'key,value')
Node = collections.namedtuple('Node', 'id, version, changeset, user, uid, visible, timestamp, lat, lon, tags')
Way = collections.namedtuple('Way', 'id, version, changeset, user, uid, visible, timestamp, nds, tags')
Relation = collections.namedtuple('Relation', 'id, version, changeset, user, uid, visible, timestamp, members, tags')
Member = collections.namedtuple('Member', 'type, ref, role')
Changeset = collections.namedtuple('Changeset', 'id, created_at, closed_at, open, min_lat, max_lat, min_lon, max_lon, user, uid, tags')
