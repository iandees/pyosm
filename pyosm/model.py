import collections

Tag = collections.namedtuple('Tag', 'key,value')
Node = collections.namedtuple('Node', 'id, version, changeset, user, uid, visible, timestamp, lat, lon, tags')
Way = collections.namedtuple('Way', 'id, version, changeset, user, uid, visible, timestamp, nds, tags')
Relation = collections.namedtuple('Relation', 'id, version, changeset, user, uid, visible, timestamp, members, tags')
Member = collections.namedtuple('Member', 'type, ref, role')
