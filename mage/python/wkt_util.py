import mgp
from pyproj import Transformer
from shapely import wkt as shapely_wkt
from shapely.geometry import Point
from shapely.ops import transform

_to_metric = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True).transform
_to_geographic = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True).transform


def _parse_wkt(wkt: str):
    try:
        return shapely_wkt.loads(wkt)
    except Exception as e:
        raise ValueError(f"Invalid WKT string: {e}")


def _metric(geom):
    return transform(_to_metric, geom)


def _geographic(geom):
    return transform(_to_geographic, geom)


# ---------------------------------------------------------------------------
# Spatial relationship predicates
# ---------------------------------------------------------------------------


@mgp.function
def ST_Contains(a_wkt: str, b_wkt: str) -> bool:
    """Return True if geometry A contains geometry B (strict)."""
    return _parse_wkt(a_wkt).contains(_parse_wkt(b_wkt))


@mgp.function
def ST_Within(a_wkt: str, b_wkt: str) -> bool:
    """Return True if geometry A is within geometry B (strict)."""
    return _parse_wkt(a_wkt).within(_parse_wkt(b_wkt))


@mgp.function
def ST_Intersects(a_wkt: str, b_wkt: str) -> bool:
    """Return True if geometries A and B share any point."""
    return _parse_wkt(a_wkt).intersects(_parse_wkt(b_wkt))


@mgp.function
def ST_Disjoint(a_wkt: str, b_wkt: str) -> bool:
    """Return True if geometries A and B share no points."""
    return _parse_wkt(a_wkt).disjoint(_parse_wkt(b_wkt))


@mgp.function
def ST_Touches(a_wkt: str, b_wkt: str) -> bool:
    """Return True if A and B touch only at their boundaries (interiors do not intersect)."""
    return _parse_wkt(a_wkt).touches(_parse_wkt(b_wkt))


@mgp.function
def ST_Crosses(a_wkt: str, b_wkt: str) -> bool:
    """Return True if the interiors of A and B cross."""
    return _parse_wkt(a_wkt).crosses(_parse_wkt(b_wkt))


@mgp.function
def ST_Overlaps(a_wkt: str, b_wkt: str) -> bool:
    """Return True if A and B have the same dimension and partially overlap."""
    return _parse_wkt(a_wkt).overlaps(_parse_wkt(b_wkt))


@mgp.function
def ST_Equals(a_wkt: str, b_wkt: str) -> bool:
    """Return True if A and B are spatially equal (regardless of vertex order)."""
    return _parse_wkt(a_wkt).equals(_parse_wkt(b_wkt))


# ---------------------------------------------------------------------------
# Distance
# ---------------------------------------------------------------------------


@mgp.function
def ST_DWithin(a_wkt: str, b_wkt: str, distance: mgp.Number) -> bool:
    """Return True if geometries A and B are within `distance` metres of each other."""
    return _metric(_parse_wkt(a_wkt)).distance(_metric(_parse_wkt(b_wkt))) <= distance


@mgp.function
def ST_Distance(a_wkt: str, b_wkt: str) -> float:
    """Return the minimum distance in metres between geometries A and B."""
    return _metric(_parse_wkt(a_wkt)).distance(_metric(_parse_wkt(b_wkt)))


# ---------------------------------------------------------------------------
# Constructors and accessors
# ---------------------------------------------------------------------------


@mgp.function
def ST_MakePoint(lon: mgp.Number, lat: mgp.Number) -> str:
    """Return WKT for a POINT at the given longitude and latitude."""
    return shapely_wkt.dumps(Point(lon, lat))


@mgp.function
def ST_X(point_wkt: str) -> float:
    """Return the longitude (X coordinate) of a POINT."""
    geom = _parse_wkt(point_wkt)
    if not isinstance(geom, Point):
        raise ValueError(f"ST_X expects POINT, got {type(geom).__name__}")
    return geom.x


@mgp.function
def ST_Y(point_wkt: str) -> float:
    """Return the latitude (Y coordinate) of a POINT."""
    geom = _parse_wkt(point_wkt)
    if not isinstance(geom, Point):
        raise ValueError(f"ST_Y expects POINT, got {type(geom).__name__}")
    return geom.y


@mgp.function
def ST_Boundary(geom_wkt: str) -> str:
    """Return the WKT of the geometry's boundary."""
    return shapely_wkt.dumps(_parse_wkt(geom_wkt).boundary)


# ---------------------------------------------------------------------------
# Measurement
# ---------------------------------------------------------------------------


@mgp.function
def ST_Length(geom_wkt: str) -> float:
    """Return the length in metres of a (multi)linestring. Returns 0 for points; perimeter for polygons."""
    return _metric(_parse_wkt(geom_wkt)).length


@mgp.function
def ST_Area(geom_wkt: str) -> float:
    """Return the area in square metres of a (multi)polygon. Returns 0 for lower-dimension geometries."""
    return _metric(_parse_wkt(geom_wkt)).area


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------


@mgp.function
def ST_Buffer(geom_wkt: str, distance: mgp.Number) -> str:
    """Return WKT for the geometry expanded by `distance` metres in all directions."""
    buffered = _metric(_parse_wkt(geom_wkt)).buffer(distance)
    return shapely_wkt.dumps(_geographic(buffered))


@mgp.function
def ST_Centroid(geom_wkt: str) -> str:
    """Return WKT for the geometric centroid of the geometry."""
    return shapely_wkt.dumps(_parse_wkt(geom_wkt).centroid)
