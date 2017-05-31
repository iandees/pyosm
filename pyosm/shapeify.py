from parsing import iter_osm_file
from model import Node, Way, Relation
from shapely.geometry import Point, LineString, Polygon
from shapely.ops import polygonize

polygon_way_tags = {
    'area': ('yes'),
    'building': ('yes'),
}

def way_is_polygon(way):
    return (way.nds[-1] == next(iter(way.nds))) and any([t.key in polygon_way_tags and t.value in polygon_way_tags[t.key] for t in way.tags])

def get_shapes(filelike):
    shapes = []
    node_cache = {}
    way_cache = {}
    for thing in iter_osm_file(filelike):
        if type(thing) == Node:
            pt = (thing.lon, thing.lat)
            shape = Point(pt)

            node_cache[thing.id] = pt

            if thing.tags:
                shapes.append((thing, shape))

        elif type(thing) == Way:
            points = []
            for nd in thing.nds:
                node_loc = node_cache.get(nd)
                if node_loc:
                    points.append(node_loc)
                else:
                    raise Exception("Way %s references node %s which is not parsed yet." % (thing.id, nd))

            if way_is_polygon(thing):
                shape = Polygon(points)
            else:
                shape = LineString(points)

            way_cache[thing.id] = points

            if any(thing.tags):
                # Only include tagged things at this point. Otherwise,
                # the shapes that are part of multipolygon relations
                # will be included twice.
                shapes.append((thing, shape))

        elif type(thing) == Relation:
            if any([t.key == 'type' and t.value == 'multipolygon' for t in thing.tags]):
                parts = []
                for member in thing.members:
                    if member.type == 'way':
                        shape = way_cache.get(member.ref)
                        if not shape:
                            raise Exception("Relation %s references way %s which is not parsed yet." % (thing.id, member.ref))

                        parts.append(shape)

                # Polygonize will return all the polygons created, so the
                # inner parts of the multipolygons will be returned twice
                # we only want the first one
                shapes.append((thing, next(polygonize(parts))))

    return shapes

if __name__ == '__main__':
    import sys

    for (thing, shape) in get_shapes(open(sys.argv[1], 'r')):
        print "%s %s = %s" % (type(thing), thing.id, shape.centroid)
