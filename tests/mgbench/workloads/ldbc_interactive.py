# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

# --- DISCLAIMER: This is NOT an official implementation of an LDBC Benchmark. ---
import inspect
import random
from datetime import datetime
from pathlib import Path

import helpers
from benchmark_context import BenchmarkContext
from workloads.base import Workload
from workloads.importers.importer_ldbc_interactive import *


class LDBC_Interactive(Workload):
    NAME = "ldbc_interactive"
    VARIANTS = ["sf0.1", "sf1", "sf3", "sf10"]
    DEFAULT_VARIANT = "sf1"

    URL_FILE = {
        "sf0.1": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/interactive/ldbc_interactive_sf0.1.cypher.gz",
        "sf1": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/interactive/ldbc_interactive_sf1.cypher.gz",
        "sf3": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/interactive/ldbc_interactive_sf3.cypher.gz",
        "sf10": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/interactive/ldbc_interactive_sf10.cypher.gz",
    }
    URL_CSV = {
        "sf0.1": "https://repository.surfsara.nl/datasets/cwi/snb/files/social_network-csv_basic/social_network-csv_basic-sf0.1.tar.zst",
        "sf1": "https://repository.surfsara.nl/datasets/cwi/snb/files/social_network-csv_basic/social_network-csv_basic-sf1.tar.zst",
        "sf3": "https://repository.surfsara.nl/datasets/cwi/snb/files/social_network-csv_basic/social_network-csv_basic-sf3.tar.zst",
        "sf10": "https://repository.surfsara.nl/datasets/cwi/snb/files/social_network-csv_basic/social_network-csv_basic-sf10.tar.zst",
    }

    SIZES = {
        "sf0.1": {"vertices": 327588, "edges": 1477965},
        "sf1": {"vertices": 3181724, "edges": 17256038},
        "sf3": {"vertices": 9281922, "edges": 52695735},
        "sf10": {"vertices": 1, "edges": 1},
    }

    URL_INDEX_FILE = {
        "memgraph": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/interactive/memgraph_interactive_index.cypher",
        "neo4j": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/interactive/neo4j_interactive_index.cypher",
    }

    PROPERTIES_ON_EDGES = True

    QUERY_PARAMETERS = {
        "sf0.1": "https://repository.surfsara.nl/datasets/cwi/snb/files/substitution_parameters/substitution_parameters-sf0.1.tar.zst",
        "sf1": "https://repository.surfsara.nl/datasets/cwi/snb/files/substitution_parameters/substitution_parameters-sf1.tar.zst",
        "sf3": "https://repository.surfsara.nl/datasets/cwi/snb/files/substitution_parameters/substitution_parameters-sf3.tar.zst",
    }

    def custom_import(self) -> bool:
        importer = ImporterLDBCInteractive(
            benchmark_context=self.benchmark_context,
            dataset_name=self.NAME,
            variant=self._variant,
            index_file=self._file_index,
            csv_dict=self.URL_CSV,
        )
        return importer.execute_import()

    def _prepare_parameters_directory(self):
        parameters = Path() / ".cache" / "datasets" / self.NAME / self._variant / "parameters"
        parameters.mkdir(parents=True, exist_ok=True)
        dir_name = self.QUERY_PARAMETERS[self._variant].split("/")[-1:][0].replace(".tar.zst", "")
        if (parameters / dir_name).exists():
            print("Files downloaded:")
            parameters = parameters / dir_name
        else:
            print("Downloading files")
            downloaded_file = helpers.download_file(self.QUERY_PARAMETERS[self._variant], parameters.absolute())
            print("Unpacking the file..." + downloaded_file)
            parameters = helpers.unpack_tar_zst(Path(downloaded_file))
        return parameters

    def _get_query_parameters(self) -> dict:
        func_name = inspect.stack()[1].function
        parameters = {}
        for file in self._parameters_dir.glob("interactive_*.txt"):
            if file.name.split("_")[1] == func_name.split("_")[-2]:
                with file.open("r") as input:
                    lines = input.readlines()
                    position = random.randint(1, len(lines) - 1)
                    header = lines[0].strip("\n").split("|")
                    data = lines[position].strip("\n").split("|")
                    for i in range(len(header)):
                        if "Date" in header[i]:
                            time = int(data[i]) / 1000
                            converted = datetime.utcfromtimestamp(time).strftime("%Y-%m-%dT%H:%M:%S")
                            parameters[header[i]] = converted
                        elif data[i].isdigit():
                            parameters[header[i]] = int(data[i])
                        else:
                            parameters[header[i]] = data[i]

        return parameters

    def __init__(self, variant: str = None, benchmark_context: BenchmarkContext = None):
        super().__init__(variant, benchmark_context=benchmark_context)
        self._parameters_dir = self._prepare_parameters_directory()
        self.benchmark_context = benchmark_context

    def benchmark__interactive__complex_query_1_analytical(self):
        memgraph = (
            """
        MATCH (p:Person {id: $personId}), (friend:Person {firstName: $firstName})
            WHERE NOT p=friend
            WITH p, friend
            MATCH path =((p)-[:KNOWS *BFS 1..3]-(friend))
            WITH min(size(path)) AS distance, friend
        ORDER BY
            distance ASC,
            friend.lastName ASC,
            toInteger(friend.id) ASC
        LIMIT 20

        MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:City)
        OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:University)-[:IS_LOCATED_IN]->(uniCity:City)
        WITH friend, collect(
            CASE uni.name
                WHEN null THEN null
                ELSE [uni.name, studyAt.classYear, uniCity.name]
            END ) AS unis, friendCity, distance

        OPTIONAL MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[:IS_LOCATED_IN]->(companyCountry:Country)
        WITH friend, collect(
            CASE company.name
                WHEN null THEN null
                ELSE [company.name, workAt.workFrom, companyCountry.name]
            END ) AS companies, unis, friendCity, distance

        RETURN
            friend.id AS friendId,
            friend.lastName AS friendLastName,
            distance AS distanceFromPerson,
            friend.birthday AS friendBirthday,
            friend.gender AS friendGender,
            friend.browserUsed AS friendBrowserUsed,
            friend.locationIP AS friendLocationIp,
            friend.email AS friendEmails,
            friend.speaks AS friendLanguages,
            friendCity.name AS friendCityName,
            unis AS friendUniversities,
            companies AS friendCompanies
        ORDER BY
            distanceFromPerson ASC,
            friendLastName ASC,
            toInteger(friendId) ASC
        LIMIT 20
        """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )
        neo4j = (
            """
            MATCH (p:Person {id: $personId}), (friend:Person {firstName: $firstName})
                WHERE NOT p=friend
                WITH p, friend
                MATCH path = shortestPath((p)-[:KNOWS*1..3]-(friend))
                WITH min(length(path)) AS distance, friend
            ORDER BY
                distance ASC,
                friend.lastName ASC,
                toInteger(friend.id) ASC
            LIMIT 20

            MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:City)
            OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:University)-[:IS_LOCATED_IN]->(uniCity:City)
            WITH friend, collect(
                CASE uni.name
                    WHEN null THEN null
                    ELSE [uni.name, studyAt.classYear, uniCity.name]
                END ) AS unis, friendCity, distance

            OPTIONAL MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[:IS_LOCATED_IN]->(companyCountry:Country)
            WITH friend, collect(
                CASE company.name
                    WHEN null THEN null
                    ELSE [company.name, workAt.workFrom, companyCountry.name]
                END ) AS companies, unis, friendCity, distance

            RETURN
                friend.id AS friendId,
                friend.lastName AS friendLastName,
                distance AS distanceFromPerson,
                friend.birthday AS friendBirthday,
                friend.gender AS friendGender,
                friend.browserUsed AS friendBrowserUsed,
                friend.locationIP AS friendLocationIp,
                friend.email AS friendEmails,
                friend.speaks AS friendLanguages,
                friendCity.name AS friendCityName,
                unis AS friendUniversities,
                companies AS friendCompanies
            ORDER BY
                distanceFromPerson ASC,
                friendLastName ASC,
                toInteger(friendId) ASC
            LIMIT 20
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )
        if self._vendor == "memgraph":
            return memgraph
        else:
            return neo4j

    def benchmark__interactive__complex_query_2_analytical(self):
        return (
            """
            MATCH (:Person {id: $personId })-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message:Message)
            WHERE message.creationDate <= localDateTime($maxDate)
            RETURN
                friend.id AS personId,
                friend.firstName AS personFirstName,
                friend.lastName AS personLastName,
                message.id AS postOrCommentId,
                coalesce(message.content,message.imageFile) AS postOrCommentContent,
                message.creationDate AS postOrCommentCreationDate
            ORDER BY
                postOrCommentCreationDate DESC,
                toInteger(postOrCommentId) ASC
            LIMIT 20
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

    def benchmark__interactive__complex_query_3_analytical(self):
        memgraph = (
            """
            MATCH (countryX:Country {name: $countryXName }),
                (countryY:Country {name: $countryYName }),
                (person:Person {id: $personId })
            WITH person, countryX, countryY
            LIMIT 1
            MATCH (city:City)-[:IS_PART_OF]->(country:Country)
            WHERE country IN [countryX, countryY]
            WITH person, countryX, countryY, collect(city) AS cities
            MATCH (person)-[:KNOWS*1..2]-(friend)-[:IS_LOCATED_IN]->(city)
            WHERE NOT person=friend AND NOT city IN cities
            WITH DISTINCT friend, countryX, countryY
            MATCH (friend)<-[:HAS_CREATOR]-(message),
                (message)-[:IS_LOCATED_IN]->(country)
            WHERE localDateTime($startDate) + duration({day:$durationDays}) > message.creationDate >= localDateTime($startDate) AND
                country IN [countryX, countryY]
            WITH friend,
                CASE WHEN country=countryX THEN 1 ELSE 0 END AS messageX,
                CASE WHEN country=countryY THEN 1 ELSE 0 END AS messageY
            WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount
            WHERE xCount>0 AND yCount>0
            RETURN friend.id AS friendId,
                friend.firstName AS friendFirstName,
                friend.lastName AS friendLastName,
                xCount,
                yCount,
                xCount + yCount AS xyCount
            ORDER BY xyCount DESC, friendId ASC
            LIMIT 20
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )
        neo4j = (
            """
            MATCH (countryX:Country {name: $countryXName }),
                (countryY:Country {name: $countryYName }),
                (person:Person {id: $personId })
            WITH person, countryX, countryY
            LIMIT 1
            MATCH (city:City)-[:IS_PART_OF]->(country:Country)
            WHERE country IN [countryX, countryY]
            WITH person, countryX, countryY, collect(city) AS cities
            MATCH (person)-[:KNOWS*1..2]-(friend)-[:IS_LOCATED_IN]->(city)
            WHERE NOT person=friend AND NOT city IN cities
            WITH DISTINCT friend, countryX, countryY
            MATCH (friend)<-[:HAS_CREATOR]-(message),
                (message)-[:IS_LOCATED_IN]->(country)
            WHERE localDateTime($startDate) + duration({days:$durationDays}) > message.creationDate >= localDateTime($startDate) AND
                country IN [countryX, countryY]
            WITH friend,
                CASE WHEN country=countryX THEN 1 ELSE 0 END AS messageX,
                CASE WHEN country=countryY THEN 1 ELSE 0 END AS messageY
            WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount
            WHERE xCount>0 AND yCount>0
            RETURN friend.id AS friendId,
                friend.firstName AS friendFirstName,
                friend.lastName AS friendLastName,
                xCount,
                yCount,
                xCount + yCount AS xyCount
            ORDER BY xyCount DESC, friendId ASC
            LIMIT 20
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

        if self._vendor == "memgraph":
            return memgraph
        else:
            return neo4j

    def benchmark__interactive__complex_query_4_analytical(self):
        memgraph = (
            """
            MATCH (person:Person {id: $personId })-[:KNOWS]-(friend:Person),
                (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag)
            WITH DISTINCT tag, post
            WITH tag,
                CASE
                    WHEN localDateTime($startDate) + duration({day:$durationDays}) > post.creationDate >= localDateTime($startDate) THEN 1
                    ELSE 0
                END AS valid,
                CASE
                    WHEN localDateTime($startDate) > post.creationDate THEN 1
                    ELSE 0
                END AS inValid
            WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
            WHERE postCount>0 AND inValidPostCount=0
            RETURN tag.name AS tagName, postCount
            ORDER BY postCount DESC, tagName ASC
            LIMIT 10
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

        neo4j = (
            """
            MATCH (person:Person {id: $personId })-[:KNOWS]-(friend:Person),
                (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag)
            WITH DISTINCT tag, post
            WITH tag,
                CASE
                    WHEN localDateTime($startDate) + duration({days:$durationDays}) > post.creationDate >= localDateTime($startDate) THEN 1
                    ELSE 0
                END AS valid,
                CASE
                    WHEN localDateTime($startDate) > post.creationDate THEN 1
                    ELSE 0
                END AS inValid
            WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
            WHERE postCount>0 AND inValidPostCount=0
            RETURN tag.name AS tagName, postCount
            ORDER BY postCount DESC, tagName ASC
            LIMIT 10
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

        if self._vendor == "memgraph":
            return memgraph
        else:
            return neo4j

    def benchmark__interactive__complex_query_5_analytical(self):
        return (
            """
            MATCH (person:Person { id: $personId })-[:KNOWS*1..2]-(friend)
            WHERE
                NOT person=friend
            WITH DISTINCT friend
            MATCH (friend)<-[membership:HAS_MEMBER]-(forum)
            WHERE
                membership.joinDate > localDateTime($minDate)
            WITH
                forum,
                collect(friend) AS friends
            OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)
            WHERE
                friend IN friends
            WITH
                forum,
                count(post) AS postCount
            RETURN
                forum.title AS forumName,
                postCount
            ORDER BY
                postCount DESC,
                forum.id ASC
            LIMIT 20
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

    def benchmark__interactive__complex_query_6_analytical(self):
        return (
            """
            MATCH (knownTag:Tag { name: $tagName })
            WITH knownTag.id as knownTagId

            MATCH (person:Person { id: $personId })-[:KNOWS*1..2]-(friend)
            WHERE NOT person=friend
            WITH
                knownTagId,
                collect(distinct friend) as friends
            UNWIND friends as f
                MATCH (f)<-[:HAS_CREATOR]-(post:Post),
                    (post)-[:HAS_TAG]->(t:Tag{id: knownTagId}),
                    (post)-[:HAS_TAG]->(tag:Tag)
                WHERE NOT t = tag
                WITH
                    tag.name as tagName,
                    count(post) as postCount
            RETURN
                tagName,
                postCount
            ORDER BY
                postCount DESC,
                tagName ASC
            LIMIT 10
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

    def benchmark__interactive__complex_query_7_analytical(self):
        memgraph = (
            """
            MATCH (person:Person {id: $personId})<-[:HAS_CREATOR]-(message:Message)<-[like:LIKES]-(liker:Person)
                WITH liker, message, like.creationDate AS likeTime, person
                ORDER BY likeTime DESC, toInteger(message.id) ASC
                WITH liker, head(collect({msg: message, likeTime: likeTime})) AS latestLike, person
                OPTIONAL MATCH (liker)-[:KNOWS]-(person)
                WITH liker, latestLike, person,
                    CASE WHEN person IS null THEN TRUE ELSE FALSE END AS isNew
            RETURN
                liker.id AS personId,
                liker.firstName AS personFirstName,
                liker.lastName AS personLastName,
                latestLike.likeTime AS likeCreationDate,
                latestLike.msg.id AS commentOrPostId,
                coalesce(latestLike.msg.content, latestLike.msg.imageFile) AS commentOrPostContent,
                (latestLike.likeTime - latestLike.msg.creationDate).minute AS minutesLatency
            ORDER BY
                likeCreationDate DESC,
                toInteger(personId) ASC
            LIMIT 20
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )
        neo4j = (
            """
            MATCH (person:Person {id: $personId})<-[:HAS_CREATOR]-(message:Message)<-[like:LIKES]-(liker:Person)
                WITH liker, message, like.creationDate AS likeTime, person
                ORDER BY likeTime DESC, toInteger(message.id) ASC
                WITH liker, head(collect({msg: message, likeTime: likeTime})) AS latestLike, person
            RETURN
                liker.id AS personId,
                liker.firstName AS personFirstName,
                liker.lastName AS personLastName,
                latestLike.likeTime AS likeCreationDate,
                latestLike.msg.id AS commentOrPostId,
                coalesce(latestLike.msg.content, latestLike.msg.imageFile) AS commentOrPostContent,
                duration.between(latestLike.likeTime, latestLike.msg.creationDate).minutes AS minutesLatency,
                not((liker)-[:KNOWS]-(person)) AS isNew
            ORDER BY
                likeCreationDate DESC,
                toInteger(personId) ASC
            LIMIT 20
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )
        if self._vendor == "memgraph":
            return memgraph
        else:
            return neo4j

    def benchmark__interactive__complex_query_8_analytical(self):
        return (
            """
            MATCH (start:Person {id: $personId})<-[:HAS_CREATOR]-(:Message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_CREATOR]->(person:Person)
            RETURN
                person.id AS personId,
                person.firstName AS personFirstName,
                person.lastName AS personLastName,
                comment.creationDate AS commentCreationDate,
                comment.id AS commentId,
                comment.content AS commentContent
            ORDER BY
                commentCreationDate DESC,
                commentId ASC
            LIMIT 20
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

    def benchmark__interactive__complex_query_9_analytical(self):
        return (
            """
            MATCH (root:Person {id: $personId })-[:KNOWS*1..2]-(friend:Person)
            WHERE NOT friend = root
            WITH collect(distinct friend) as friends
            UNWIND friends as friend
                MATCH (friend)<-[:HAS_CREATOR]-(message:Message)
                WHERE message.creationDate < localDateTime($maxDate)
            RETURN
                friend.id AS personId,
                friend.firstName AS personFirstName,
                friend.lastName AS personLastName,
                message.id AS commentOrPostId,
                coalesce(message.content,message.imageFile) AS commentOrPostContent,
                message.creationDate AS commentOrPostCreationDate
            ORDER BY
                commentOrPostCreationDate DESC,
                message.id ASC
            LIMIT 20
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

    def benchmark__interactive__complex_query_11_analytical(self):
        return (
            """
            MATCH (person:Person {id: $personId })-[:KNOWS*1..2]-(friend:Person)
            WHERE not(person=friend)
            WITH DISTINCT friend
            MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[:IS_LOCATED_IN]->(:Country {name: $countryName })
            WHERE workAt.workFrom < $workFromYear
            RETURN
                    friend.id AS personId,
                    friend.firstName AS personFirstName,
                    friend.lastName AS personLastName,
                    company.name AS organizationName,
                    workAt.workFrom AS organizationWorkFromYear
            ORDER BY
                    organizationWorkFromYear ASC,
                    toInteger(personId) ASC,
                    organizationName DESC
            LIMIT 10
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

    def benchmark__interactive__complex_query_12_analytical(self):
        return (
            """
            MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
            WHERE tag.name = $tagClassName OR baseTagClass.name = $tagClassName
            WITH collect(tag.id) as tags
            MATCH (:Person {id: $personId })-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(:Post)-[:HAS_TAG]->(tag:Tag)
            WHERE tag.id in tags
            RETURN
                friend.id AS personId,
                friend.firstName AS personFirstName,
                friend.lastName AS personLastName,
                collect(DISTINCT tag.name) AS tagNames,
                count(DISTINCT comment) AS replyCount
            ORDER BY
                replyCount DESC,
                toInteger(personId) ASC
            LIMIT 20
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

    def benchmark__interactive__complex_query_13_analytical(self):
        memgraph = (
            """
            MATCH
                (person1:Person {id: $person1Id}),
                (person2:Person {id: $person2Id}),
                path = (person1)-[:KNOWS *BFS]-(person2)
            RETURN
                CASE path IS NULL
                    WHEN true THEN -1
                    ELSE size(path)
                END AS shortestPathLength
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

        neo4j = (
            """
            MATCH
                (person1:Person {id: $person1Id}),
                (person2:Person {id: $person2Id}),
                path = shortestPath((person1)-[:KNOWS*]-(person2))
            RETURN
                CASE path IS NULL
                    WHEN true THEN -1
                    ELSE length(path)
                END AS shortestPathLength
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

        if self._vendor == "memgraph":
            return memgraph
        else:
            return neo4j
