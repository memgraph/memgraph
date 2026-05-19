import mgp
from pyproj import Transformer
from shapely import wkt as shapely_wkt
from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, Polygon
from shapely.ops import transform

_to_metric = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True).transform


def _parse_wkt(wkt: str):
    try:
        return shapely_wkt.loads(wkt)
    except Exception as e:
        raise ValueError(f"Invalid WKT string: {e}")


def _to_wkt(geom) -> str:
    return shapely_wkt.dumps(geom)


def _assert_type(geom, *expected_types, param: str = "wkt"):
    if not isinstance(geom, expected_types):
        names = ", ".join(t.__name__ for t in expected_types)
        raise ValueError(f"Expected {names} for '{param}', got {type(geom).__name__}")


def _metric(geom):
    return transform(_to_metric, geom)


# ---------------------------------------------------------------------------
# POINT predicates
# ---------------------------------------------------------------------------


@mgp.function
def point_within_polygon(point_wkt: str, polygon_wkt: str) -> bool:
    """Return True if the POINT lies strictly inside the POLYGON."""
    point = _parse_wkt(point_wkt)
    _assert_type(point, Point, param="point_wkt")
    polygon = _parse_wkt(polygon_wkt)
    _assert_type(polygon, Polygon, MultiPolygon, param="polygon_wkt")
    return polygon.contains(point)


@mgp.function
def point_within_distance_of_polygon_boundary(point_wkt: str, polygon_wkt: str, distance: mgp.Number) -> bool:
    """Return True if the POINT is within `distance` metres of the POLYGON boundary."""
    point = _parse_wkt(point_wkt)
    _assert_type(point, Point, param="point_wkt")
    polygon = _parse_wkt(polygon_wkt)
    _assert_type(polygon, Polygon, MultiPolygon, param="polygon_wkt")
    return _metric(point).distance(_metric(polygon.boundary)) <= distance


@mgp.function
def point_intersects_linestring(point_wkt: str, linestring_wkt: str) -> bool:
    """Return True if the POINT lies exactly on the LINESTRING."""
    point = _parse_wkt(point_wkt)
    _assert_type(point, Point, param="point_wkt")
    line = _parse_wkt(linestring_wkt)
    _assert_type(line, LineString, MultiLineString, param="linestring_wkt")
    return line.intersects(point)


@mgp.function
def point_within_distance_of_linestring(point_wkt: str, linestring_wkt: str, distance: mgp.Number) -> bool:
    """Return True if the POINT is within `distance` metres of the LINESTRING."""
    point = _parse_wkt(point_wkt)
    _assert_type(point, Point, param="point_wkt")
    line = _parse_wkt(linestring_wkt)
    _assert_type(line, LineString, MultiLineString, param="linestring_wkt")
    return _metric(point).distance(_metric(line)) <= distance


# ---------------------------------------------------------------------------
# LINESTRING predicates
# ---------------------------------------------------------------------------


@mgp.function
def linestring_within_polygon(linestring_wkt: str, polygon_wkt: str) -> bool:
    """Return True if the entire LINESTRING is contained within the POLYGON."""
    line = _parse_wkt(linestring_wkt)
    _assert_type(line, LineString, MultiLineString, param="linestring_wkt")
    polygon = _parse_wkt(polygon_wkt)
    _assert_type(polygon, Polygon, MultiPolygon, param="polygon_wkt")
    return polygon.contains(line)


@mgp.function
def linestring_intersects_polygon(linestring_wkt: str, polygon_wkt: str) -> bool:
    """Return True if the LINESTRING partially or fully intersects the POLYGON."""
    line = _parse_wkt(linestring_wkt)
    _assert_type(line, LineString, MultiLineString, param="linestring_wkt")
    polygon = _parse_wkt(polygon_wkt)
    _assert_type(polygon, Polygon, MultiPolygon, param="polygon_wkt")
    return polygon.intersects(line)


@mgp.read_proc
def linestring_nearby_points(
    ctx: mgp.ProcCtx,
    linestring_wkt: str,
    distance: mgp.Number,
    label: str,
    property: str = "wkt",
) -> mgp.Record(vertex=mgp.Vertex):
    """
    Return every node with `label` whose `property` WKT POINT is within
    `distance` metres of the LINESTRING.
    """
    line = _parse_wkt(linestring_wkt)
    _assert_type(line, LineString, MultiLineString, param="linestring_wkt")
    line_m = _metric(line)
    result = []
    for vertex in ctx.graph.vertices:
        if not any(l.name == label for l in vertex.labels):
            continue
        wkt_val = vertex.properties.get(property)
        if not isinstance(wkt_val, str):
            continue
        try:
            point = _parse_wkt(wkt_val)
            _assert_type(point, Point)
            if _metric(point).distance(line_m) <= distance:
                result.append(mgp.Record(vertex=vertex))
        except Exception:
            continue
    return result


# ---------------------------------------------------------------------------
# POLYGON queries
# ---------------------------------------------------------------------------


@mgp.read_proc
def polygon_contained_points(
    ctx: mgp.ProcCtx,
    polygon_wkt: str,
    label: str,
    property: str = "wkt",
) -> mgp.Record(vertex=mgp.Vertex):
    """Return every node with `label` whose `property` POINT lies inside the POLYGON."""
    polygon = _parse_wkt(polygon_wkt)
    _assert_type(polygon, Polygon, MultiPolygon, param="polygon_wkt")
    result = []
    for vertex in ctx.graph.vertices:
        if not any(l.name == label for l in vertex.labels):
            continue
        wkt_val = vertex.properties.get(property)
        if not isinstance(wkt_val, str):
            continue
        try:
            point = _parse_wkt(wkt_val)
            _assert_type(point, Point)
            if polygon.contains(point):
                result.append(mgp.Record(vertex=vertex))
        except Exception:
            continue
    return result


@mgp.read_proc
def polygon_nearby_points(
    ctx: mgp.ProcCtx,
    polygon_wkt: str,
    distance: mgp.Number,
    label: str,
    property: str = "wkt",
) -> mgp.Record(vertex=mgp.Vertex):
    """
    Return every node with `label` whose `property` POINT is within
    `distance` metres of the POLYGON boundary.
    """
    polygon = _parse_wkt(polygon_wkt)
    _assert_type(polygon, Polygon, MultiPolygon, param="polygon_wkt")
    boundary_m = _metric(polygon.boundary)
    result = []
    for vertex in ctx.graph.vertices:
        if not any(l.name == label for l in vertex.labels):
            continue
        wkt_val = vertex.properties.get(property)
        if not isinstance(wkt_val, str):
            continue
        try:
            point = _parse_wkt(wkt_val)
            _assert_type(point, Point)
            if _metric(point).distance(boundary_m) <= distance:
                result.append(mgp.Record(vertex=vertex))
        except Exception:
            continue
    return result


@mgp.read_proc
def polygon_contained_linestrings(
    ctx: mgp.ProcCtx,
    polygon_wkt: str,
    label: str,
    property: str = "wkt",
    partial: bool = True,
) -> mgp.Record(vertex=mgp.Vertex):
    """
    Return every node with `label` whose `property` LINESTRING is wholly
    contained in the POLYGON, or — when `partial=True` — also partially
    intersecting it.
    """
    polygon = _parse_wkt(polygon_wkt)
    _assert_type(polygon, Polygon, MultiPolygon, param="polygon_wkt")
    result = []
    for vertex in ctx.graph.vertices:
        if not any(l.name == label for l in vertex.labels):
            continue
        wkt_val = vertex.properties.get(property)
        if not isinstance(wkt_val, str):
            continue
        try:
            line = _parse_wkt(wkt_val)
            _assert_type(line, LineString, MultiLineString)
            matched = polygon.intersects(line) if partial else polygon.contains(line)
            if matched:
                result.append(mgp.Record(vertex=vertex))
        except Exception:
            continue
    return result
