import functools
import math

import mgp
import numpy as np
import shapely
from shapely import wkt as shapely_wkt
from shapely.geometry import Point

# We deliberately do NOT use pyproj/PROJ here: PROJ's context and its proj.db
# handle cannot be initialized on Memgraph's embedded-interpreter query worker
# threads — constructing a CRS there segfaults inside native PROJ code, and no
# Python-side locking or thread-local caching avoids it.
#
# Instead of a fixed global projection we measure lengths, areas and distances
# in a local Azimuthal Equidistant (AEQD) projection centred on the geometry
# being measured (or, for a pair, on the midpoint of the two centroids). A fixed
# Web Mercator projection inflates distances by ~1/cos(lat) and areas by
# ~1/cos(lat)^2 (≈1.64x and ≈2.7x at Berlin's latitude); a data-centred AEQD
# keeps distances near-true — radial scale is exactly 1 and tangential scale is
# c/sin(c) ≈ 1 for regional extents (well under 0.1% out to a few hundred km) —
# using only closed-form spherical math. No PROJ dependency, no thread-safety
# problem. The sphere approximation itself costs ~0.1-0.3% versus the WGS84
# ellipsoid, which is well within this module's "urban-scale" accuracy promise.
_EARTH_RADIUS_M = 6371008.8  # IUGG mean Earth radius; sphere for the local metric.


def _aeqd_forward(lon0, lat0):
    """Build an AEQD (lon, lat degrees) -> (x, y metres) transform centred at (lon0, lat0).

    Operates on an (N, 2) coordinate array so shapely.transform can vectorise it.
    """
    lon0r, lat0r = math.radians(lon0), math.radians(lat0)
    sin_lat0, cos_lat0 = math.sin(lat0r), math.cos(lat0r)

    def fwd(coords):
        lon = np.radians(coords[:, 0])
        lat = np.radians(coords[:, 1])
        cos_lat, sin_lat = np.cos(lat), np.sin(lat)
        dlon = lon - lon0r
        cos_dlon, sin_dlon = np.cos(dlon), np.sin(dlon)
        cos_c = np.clip(sin_lat0 * sin_lat + cos_lat0 * cos_lat * cos_dlon, -1.0, 1.0)
        c = np.arccos(cos_c)
        k = 1.0 / np.sinc(c / np.pi)  # c / sin(c), and -> 1 as c -> 0
        x = _EARTH_RADIUS_M * k * cos_lat * sin_dlon
        y = _EARTH_RADIUS_M * k * (cos_lat0 * sin_lat - sin_lat0 * cos_lat * cos_dlon)
        return np.column_stack([x, y])

    return fwd


def _aeqd_inverse(lon0, lat0):
    """Build the inverse AEQD (x, y metres) -> (lon, lat degrees) transform centred at (lon0, lat0).

    Operates on an (N, 2) coordinate array so shapely.transform can vectorise it.
    """
    lon0r, lat0r = math.radians(lon0), math.radians(lat0)
    sin_lat0, cos_lat0 = math.sin(lat0r), math.cos(lat0r)

    def inv(coords):
        x, y = coords[:, 0], coords[:, 1]
        rho = np.hypot(x, y)
        c = rho / _EARTH_RADIUS_M
        sin_c, cos_c = np.sin(c), np.cos(c)
        safe_rho = np.where(rho == 0.0, 1.0, rho)  # avoid 0/0; masked out below
        sin_term = np.where(rho == 0.0, 0.0, y * sin_c * cos_lat0 / safe_rho)
        lat = np.arcsin(np.clip(cos_c * sin_lat0 + sin_term, -1.0, 1.0))
        lon = lon0r + np.arctan2(x * sin_c, rho * cos_lat0 * cos_c - y * sin_lat0 * sin_c)
        return np.column_stack([np.degrees(lon), np.degrees(lat)])

    return inv


def _center(*geoms):
    """Projection centre (lon, lat) for the given geometries: the mean of their centroids."""
    lons, lats = [], []
    for g in geoms:
        c = g.centroid
        if not c.is_empty:
            lons.append(c.x)
            lats.append(c.y)
    if not lons:
        return 0.0, 0.0
    return sum(lons) / len(lons), sum(lats) / len(lats)


# Parsed geometries are immutable and never mutated downstream, so the same WKT
# string can safely share one parsed object. In a query like
# `MATCH (a),(b) WHERE ST_DWithin(a.wkt, $const, d)` this parses the constant
# argument once instead of once per row. lru_cache is thread-safe in CPython.
@functools.lru_cache(maxsize=4096)
def _parse_wkt(wkt: str):
    try:
        return shapely_wkt.loads(wkt)
    except Exception as e:
        raise ValueError(f"Invalid WKT string: {e}")


def _metric(geom, lon0, lat0):
    """Project a geometry into the local AEQD metric space centred at (lon0, lat0)."""
    return shapely.transform(geom, _aeqd_forward(lon0, lat0))


def _geographic(geom, lon0, lat0):
    """Inverse of _metric: bring an AEQD-projected geometry back to lon/lat."""
    return shapely.transform(geom, _aeqd_inverse(lon0, lat0))


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
    a, b = _parse_wkt(a_wkt), _parse_wkt(b_wkt)
    lon0, lat0 = _center(a, b)
    return _metric(a, lon0, lat0).distance(_metric(b, lon0, lat0)) <= distance


@mgp.function
def ST_Distance(a_wkt: str, b_wkt: str) -> float:
    """Return the minimum distance in metres between geometries A and B."""
    a, b = _parse_wkt(a_wkt), _parse_wkt(b_wkt)
    lon0, lat0 = _center(a, b)
    return _metric(a, lon0, lat0).distance(_metric(b, lon0, lat0))


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
    g = _parse_wkt(geom_wkt)
    lon0, lat0 = _center(g)
    return _metric(g, lon0, lat0).length


@mgp.function
def ST_Area(geom_wkt: str) -> float:
    """Return the area in square metres of a (multi)polygon. Returns 0 for lower-dimension geometries."""
    g = _parse_wkt(geom_wkt)
    lon0, lat0 = _center(g)
    return _metric(g, lon0, lat0).area


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------


@mgp.function
def ST_Buffer(geom_wkt: str, distance: mgp.Number) -> str:
    """Return WKT for the geometry expanded by `distance` metres in all directions."""
    g = _parse_wkt(geom_wkt)
    lon0, lat0 = _center(g)
    buffered = _metric(g, lon0, lat0).buffer(distance)
    return shapely_wkt.dumps(_geographic(buffered, lon0, lat0))


@mgp.function
def ST_Centroid(geom_wkt: str) -> str:
    """Return WKT for the geometric centroid of the geometry."""
    return shapely_wkt.dumps(_parse_wkt(geom_wkt).centroid)


@mgp.function
def ST_Envelope(geom_wkt: str) -> str:
    """Return WKT for the axis-aligned bounding rectangle of the geometry."""
    return shapely_wkt.dumps(_parse_wkt(geom_wkt).envelope)


@mgp.function
def ST_ConvexHull(geom_wkt: str) -> str:
    """Return WKT for the convex hull of the geometry."""
    return shapely_wkt.dumps(_parse_wkt(geom_wkt).convex_hull)


# ---------------------------------------------------------------------------
# Set operations
# ---------------------------------------------------------------------------


@mgp.function
def ST_Intersection(a_wkt: str, b_wkt: str) -> str:
    """Return WKT for the geometric intersection of A and B (may be empty)."""
    return shapely_wkt.dumps(_parse_wkt(a_wkt).intersection(_parse_wkt(b_wkt)))


@mgp.function
def ST_Difference(a_wkt: str, b_wkt: str) -> str:
    """Return WKT for the part of A that does not lie in B."""
    return shapely_wkt.dumps(_parse_wkt(a_wkt).difference(_parse_wkt(b_wkt)))
