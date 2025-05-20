"""
Random graph generator for the Country / Filing / Entity schema.
Requires:
    pip install faker python-dateutil
Outputs:
    Cypher statements on stdout – redirect to a file if you like:
        python make_graph.py > load.cypher
"""

import random
from datetime import datetime, timedelta
from dateutil.tz import tzutc
from faker import Faker

# ---------------------------------------------------------------------------
# tunables – change these to make a bigger or smaller graph
NUM_COUNTRIES = 20
NUM_ENTITIES  = 150
NUM_FILINGS   = 400
SEED          = 42          # remove or change for different results
# ---------------------------------------------------------------------------

fake = Faker()
random.seed(SEED)

ISO_CODES  = set()          # keep ISO-3 codes unique


def iso3() -> str:
    """Return a unique random three-letter ISO code."""
    while True:
        code = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=3))
        if code not in ISO_CODES:
            ISO_CODES.add(code)
            return code


def random_point() -> str:
    """Return a Cypher point literal string."""
    lat = random.uniform(-90, 90)
    lng = random.uniform(-180, 180)
    return f"point({{latitude:{lat:.6f}, longitude:{lng:.6f}, srid:4326}})"


def dt_between(start: str, end: str) -> datetime:
    """Random UTC datetime between two ISO-8601 strings."""
    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
    end_dt   = datetime.fromisoformat(end.replace("Z", "+00:00"))
    delta    = end_dt - start_dt
    return start_dt + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def iso_z(dt: datetime) -> str:
    """Return datetime in ISO-8601 + 'Z'."""
    return dt.astimezone(tzutc()).isoformat(timespec="seconds").replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# generate Countries
countries = []
for i in range(NUM_COUNTRIES):
    code = iso3()
    name = fake.country()
    tld  = code[:2].upper()
    countries.append(
        {
            "id": f"c{i}",
            "code": code,
            "name": name,
            "tld": tld,
            "location": random_point(),
        }
    )

# generate Entities
entities = []
for i in range(NUM_ENTITIES):
    c = random.choice(countries)
    eid = fake.slug()
    entities.append(
        {
            "id": eid,
            "name": fake.company().rstrip(","),
            "country": c["code"],
            "location": random_point(),
        }
    )

# generate Filings
filings = []
for i in range(NUM_FILINGS):
    begin_dt = dt_between("2000-02-08T00:00:00Z", "2017-09-05T00:00:00Z")
    end_dt   = dt_between(iso_z(begin_dt), "2017-11-03T00:00:00Z")

    origin_bank = fake.company().rstrip(",")
    bene_bank   = fake.company().rstrip(",")
    origin = random.choice(countries)
    bene   = random.choice(countries)

    filings.append(
        {
            "id": str(fake.unique.random_int(min=1, max=9_999_999)),
            "begin": iso_z(begin_dt),
            "end": iso_z(end_dt),
            "begin_date_format": begin_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_date_format": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "begin_date": begin_dt.strftime("%b %d, %Y"),
            "end_date": end_dt.strftime("%b %d, %Y"),
            "originator_bank": origin_bank,
            "originator_bank_id": fake.slug(),
            "originator_bank_country": origin["name"],
            "originator_iso": origin["code"],
            "origin_lat": f"{random.uniform(-90, 90):.4f}",
            "origin_lng": f"{random.uniform(-180, 180):.4f}",
            "beneficiary_bank": bene_bank,
            "beneficiary_bank_id": fake.slug(),
            "beneficiary_bank_country": bene["name"],
            "beneficiary_iso": bene["code"],
            "beneficiary_lat": f"{random.uniform(-90, 90):.4f}",
            "beneficiary_lng": f"{random.uniform(-180, 180):.4f}",
            "amount": round(random.uniform(1.18, 2_721_000_000), 2),
            "number": random.randint(1, 174),
            # choose the entity relationships later
        }
    )

# ---------------------------------------------------------------------------
# utility – Cypher value rendering
def cypher_str(s: str) -> str:
    return '"' + s.replace('"', '\\"') + '"'


def props(data: dict, exclude=()) -> str:
    fields = []
    for k, v in data.items():
        if k in exclude:
            continue
        if isinstance(v, str) and v.startswith("point("):
            fields.append(f"{k}:{v}")
        elif isinstance(v, (int, float)):
            fields.append(f"{k}:{v}")
        else:
            fields.append(f"{k}:{cypher_str(v)}")
    return "{" + ", ".join(fields) + "}"


# ---------------------------------------------------------------------------
# emit Cypher
print("// ------------------ Countries ------------------")
for c in countries:
    print(f"CREATE (:Country {props(c, exclude=['id'])});")

print("\n// ------------------ Entities -------------------")
for e in entities:
    print(f"CREATE (:Entity {props(e)});")

print("\n// ------------------ Filings --------------------")
for f in filings:
    print(f"CREATE (:Filing {props(f)});")

print("\n// ------------------ Relationships --------------")
for f in filings:
    # pick entities for the three roles
    filing_id  = f["id"]
    originator = random.choice(entities)["id"]
    beneficiary = random.choice(entities)["id"]
    concern     = random.choice(entities)["id"]

    # FILED
    print(f"MATCH (e:Entity {{id:{cypher_str(originator)}}}), (f:Filing {{id:{cypher_str(filing_id)}}}) "
          f"CREATE (e)-[:FILED]->(f);")
    # ORIGINATOR
    print(f"MATCH (f:Filing {{id:{cypher_str(filing_id)}}}), (e:Entity {{id:{cypher_str(originator)}}}) "
          f"CREATE (f)-[:ORIGINATOR]->(e);")
    # BENEFITS
    print(f"MATCH (f:Filing {{id:{cypher_str(filing_id)}}}), (e:Entity {{id:{cypher_str(beneficiary)}}}) "
          f"CREATE (f)-[:BENEFITS]->(e);")
    # CONCERNS
    print(f"MATCH (f:Filing {{id:{cypher_str(filing_id)}}}), (e:Entity {{id:{cypher_str(concern)}}}) "
          f"CREATE (f)-[:CONCERNS]->(e);")

# Entity-COUNTRY relations
print("\n// --------------- Entity → Country --------------")
for e in entities:
    print(
        f"MATCH (e:Entity {{id:{cypher_str(e['id'])}}}), (c:Country {{code:{cypher_str(e['country'])}}}) "
        f"CREATE (e)-[:COUNTRY]->(c);"
    )

print("// ------------------ Done -----------------------")
