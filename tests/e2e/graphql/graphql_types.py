import sys
from textwrap import dedent

import pytest
from graphql_server import *


@pytest.fixture
def query_server() -> GraphQLServer:
    return GraphQLServer("./types.js")


def test_types(query_server):
    query = dedent(
        """\
        mutation CreateNode {
            createNs(
                input: [
                    {
                        id: "001"
                        name: "Every Type"
                        description: "Contains all supported GraphQL data types"
                        isActive: true
                        age: 42
                        score: 98.75
                        largeNumber: "9223372036854775807"
                        birthDate: "1982-03-15"
                        localMeetingTime: "09:15:30"
                        createdAt: "2024-07-04T10:30:00.654321Z"
                        lastModified: "2024-07-04T15:45:20"
                        sessionDuration: "PT2H45M30S"
                        location: { latitude: 51.5074, longitude: -0.1278, height: 45.0 }
                        coordinates: { x: 150.5, y: 300.2, z: 75.8 }
                    }
                ]
            ) {
                ns {
                    id
                }
            }
        }
        """
    ).strip()

    gotten = query_server.send_query(query)
    expected_result = dedent(
        """\
        {
            "data": {
                "createNs": {
                "ns": [
                        {
                            "id": "001"
                        }
                    ]
                }
            }
        }
        """
    ).strip()
    assert server_returned_expected(expected_result, gotten)

    query = dedent(
        """\
        query Ns {
            ns {
                age
                birthDate
                coordinates {
                    crs
                    srid
                    x
                    y
                    z
                }
                createdAt
                description
                id
                isActive
                largeNumber
                lastModified
                localMeetingTime
                location {
                    crs
                    height
                    latitude
                    longitude
                    srid
                }
                name
                score
                sessionDuration
            }
        }
        """
    ).strip()

    gotten = query_server.send_query(query)
    expected_result = dedent(
        """\
        {
            "data": {
                "ns": [
                    {
                        "age": 42,
                        "birthDate": "1982-03-15",
                        "coordinates": {
                            "crs": "cartesian-3d",
                            "srid": 9157,
                            "x": 150.5,
                            "y": 300.2,
                            "z": 75.8
                        },
                        "createdAt": "2024-07-04T10:30:00.654Z",
                        "description": "Contains all supported GraphQL data types",
                        "id": "001",
                        "isActive": true,
                        "largeNumber": "9223372036854775807",
                        "lastModified": "2024-07-04T15:45:20",
                        "localMeetingTime": "09:15:30",
                        "location": {
                            "crs": "wgs-84-3d",
                            "height": 45,
                            "latitude": 51.5074,
                            "longitude": -0.1278,
                            "srid": 4979
                        },
                        "name": "Every Type",
                        "score": 98.75,
                        "sessionDuration": "P0M0DT9930S"
                    }
                ]
            }
        }
        """
    ).strip()
    assert server_returned_expected(expected_result, gotten)

    query_server.send_query("mutation { teardown }")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
