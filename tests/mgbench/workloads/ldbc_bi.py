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

# ---  DISCLAIMER: This is NOT an official implementation of an LDBC Benchmark.  ---
import inspect
import random
from pathlib import Path

import helpers
from benchmark_context import BenchmarkContext
from workloads.base import Workload
from workloads.importers.importer_ldbc_bi import ImporterLDBCBI


class LDBC_BI(Workload):
    NAME = "ldbc_bi"
    VARIANTS = ["sf1", "sf3", "sf10"]
    DEFAULT_VARIANT = "sf1"

    URL_FILE = {
        "sf1": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/bi/ldbc_bi_sf1.cypher.gz",
        "sf3": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/bi/ldbc_bi_sf3.cypher.gz",
        "sf10": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/bi/ldbc_bi_sf10.cypher.gz",
    }

    URL_CSV = {
        "sf1": "https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/bi-sf1-composite-projected-fk.tar.zst",
        "sf3": "https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/bi-sf3-composite-projected-fk.tar.zst",
        "sf10": "https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/bi-sf10-composite-projected-fk.tar.zst",
    }

    SIZES = {
        "sf1": {"vertices": 2997352, "edges": 17196776},
        "sf3": {"vertices": 1, "edges": 1},
        "sf10": {"vertices": 1, "edges": 1},
    }

    LOCAL_INDEX_FILES = None

    URL_INDEX_FILE = {
        "memgraph": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/bi/memgraph_bi_index.cypher",
        "neo4j": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/bi/neo4j_bi_index.cypher",
    }

    QUERY_PARAMETERS = {
        "sf1": "https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/parameters-2022-10-01.zip",
        "sf3": "https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/parameters-2022-10-01.zip",
        "sf10": "https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/parameters-2022-10-01.zip",
    }

    def custom_import(self) -> bool:
        importer = ImporterLDBCBI(
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
        if parameters.exists() and any(parameters.iterdir()):
            print("Files downloaded.")
        else:
            print("Downloading files")
            downloaded_file = helpers.download_file(self.QUERY_PARAMETERS[self._variant], parameters.parent.absolute())
            print("Unpacking the file..." + downloaded_file)
            helpers.unpack_zip(Path(downloaded_file))
        return parameters / ("parameters-" + self._variant)

    def _get_query_parameters(self) -> dict:
        func_name = inspect.stack()[1].function
        parameters = {}
        for file in self._parameters_dir.glob("bi-*.csv"):
            file_name_query_id = file.name.split("-")[1][0:-4]
            func_name_id = func_name.split("_")[-2]
            if file_name_query_id == func_name_id or file_name_query_id == func_name_id + "a":
                with file.open("r") as input:
                    lines = input.readlines()
                    header = lines[0].strip("\n").split("|")
                    position = random.randint(1, len(lines) - 1)
                    data = lines[position].strip("\n").split("|")
                    for i in range(len(header)):
                        key, value_type = header[i].split(":")
                        if value_type == "DATETIME":
                            # Drop time zone
                            converted = data[i][0:-6]
                            parameters[key] = converted
                        elif value_type == "DATE":
                            converted = data[i] + "T00:00:00"
                            parameters[key] = converted
                        elif value_type == "INT":
                            parameters[key] = int(data[i])
                        elif value_type == "STRING[]":
                            elements = data[i].split(";")
                            parameters[key] = elements
                        else:
                            parameters[key] = data[i]
                break

        return parameters

    def __init__(self, variant=None, benchmark_context: BenchmarkContext = None):
        super().__init__(variant, benchmark_context=benchmark_context)
        self._parameters_dir = self._prepare_parameters_directory()

    def benchmark__bi__query_1_analytical(self):
        memgraph = (
            """
            MATCH (message:Message)
            WHERE message.creationDate < localDateTime($datetime)
            WITH count(message) AS totalMessageCountInt
            WITH toFloat(totalMessageCountInt) AS totalMessageCount
            MATCH (message:Message)
            WHERE message.creationDate < localDateTime($datetime)
            AND message.content IS NOT NULL
            WITH
                totalMessageCount,
                message,
                message.creationDate.year AS year
            WITH
                totalMessageCount,
                year,
                message:Comment AS isComment,
                CASE
                    WHEN message.length <  40 THEN 0
                    WHEN message.length <  80 THEN 1
                    WHEN message.length < 160 THEN 2
                    ELSE                           3
                END AS lengthCategory,
                count(message) AS messageCount,
                sum(message.length) / toFloat(count(message)) AS averageMessageLength,
                sum(message.length) AS sumMessageLength
            RETURN
                year,
                isComment,
                lengthCategory,
                messageCount,
                averageMessageLength,
                sumMessageLength,
                messageCount / totalMessageCount AS percentageOfMessages
            ORDER BY
                year DESC,
                isComment ASC,
                lengthCategory ASC
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

        neo4j = (
            """
            MATCH (message:Message)
            WHERE message.creationDate < DateTime($datetime)
            WITH count(message) AS totalMessageCountInt
            WITH toFloat(totalMessageCountInt) AS totalMessageCount
            MATCH (message:Message)
            WHERE message.creationDate < DateTime($datetime)
            AND message.content IS NOT NULL
            WITH
                totalMessageCount,
                message,
                message.creationDate.year AS year
            WITH
                totalMessageCount,
                year,
                message:Comment AS isComment,
                CASE
                    WHEN message.length <  40 THEN 0
                    WHEN message.length <  80 THEN 1
                    WHEN message.length < 160 THEN 2
                    ELSE                           3
                END AS lengthCategory,
                count(message) AS messageCount,
                sum(message.length) / toFloat(count(message)) AS averageMessageLength,
                sum(message.length) AS sumMessageLength
            RETURN
                year,
                isComment,
                lengthCategory,
                messageCount,
                averageMessageLength,
                sumMessageLength,
                messageCount / totalMessageCount AS percentageOfMessages
            ORDER BY
                year DESC,
                isComment ASC,
                lengthCategory ASC
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )
        if self._vendor == "memgraph":
            return memgraph
        else:
            return neo4j

    def benchmark__bi__query_2_analytical(self):
        memgraph = (
            """
            MATCH (tag:Tag)-[:HAS_TYPE]->(:TagClass {name: $tagClass})
            OPTIONAL MATCH (message1:Message)-[:HAS_TAG]->(tag)
            WHERE localDateTime($date) <= message1.creationDate
                AND message1.creationDate < localDateTime($date) + duration({day: 100})
            WITH tag, count(message1) AS countWindow1
            OPTIONAL MATCH (message2:Message)-[:HAS_TAG]->(tag)
            WHERE localDateTime($date) + duration({day: 100}) <= message2.creationDate
                AND message2.creationDate < localDateTime($date) + duration({day: 200})
            WITH
                tag,
                countWindow1,
                count(message2) AS countWindow2
            RETURN
                tag.name,
                countWindow1,
                countWindow2,
                abs(countWindow1 - countWindow2) AS diff
            ORDER BY
                diff DESC,
                tag.name ASC
            LIMIT 100
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

        neo4j = (
            """
            MATCH (tag:Tag)-[:HAS_TYPE]->(:TagClass {name: $tagClass})
            OPTIONAL MATCH (message1:Message)-[:HAS_TAG]->(tag)
            WHERE DateTime($date) <= message1.creationDate
                AND message1.creationDate < DateTime($date) + duration({days: 100})
            WITH tag, count(message1) AS countWindow1
            OPTIONAL MATCH (message2:Message)-[:HAS_TAG]->(tag)
            WHERE DateTime($date) + duration({days: 100}) <= message2.creationDate
                AND message2.creationDate < DateTime($date) + duration({days: 200})
            WITH
                tag,
                countWindow1,
                count(message2) AS countWindow2
            RETURN
                tag.name,
                countWindow1,
                countWindow2,
                abs(countWindow1 - countWindow2) AS diff
            ORDER BY
                diff DESC,
                tag.name ASC
            LIMIT 100
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )
        if self._vendor == "memgraph":
            return memgraph
        else:
            return neo4j

    def benchmark__bi__query_3_analytical(self):
        return (
            """
            MATCH
                (:Country {name: $country})<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-
                (person:Person)<-[:HAS_MODERATOR]-(forum:Forum)-[:CONTAINER_OF]->
                (post:Post)<-[:REPLY_OF*0..]-(message:Message)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass {name: $tagClass})
            RETURN
                forum.id as id,
                forum.title,
                person.id,
                count(DISTINCT message) AS messageCount
            ORDER BY
                messageCount DESC,
                id ASC
            LIMIT 20
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

    def benchmark__bi__query_5_analytical(self):
        return (
            """
            MATCH (tag:Tag {name: $tag})<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person:Person)
            OPTIONAL MATCH (message)<-[likes:LIKES]-(:Person)
            WITH person, message, count(likes) AS likeCount
            OPTIONAL MATCH (message)<-[:REPLY_OF]-(reply:Comment)
            WITH person, message, likeCount, count(reply) AS replyCount
            WITH person, count(message) AS messageCount, sum(likeCount) AS likeCount, sum(replyCount) AS replyCount
            RETURN
                person.id,
                replyCount,
                likeCount,
                messageCount,
                1*messageCount + 2*replyCount + 10*likeCount AS score
            ORDER BY
                score DESC,
                person.id ASC
            LIMIT 100
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

    def benchmark__bi__query_6_analytical(self):
        return (
            """
            MATCH (tag:Tag {name: $tag})<-[:HAS_TAG]-(message1:Message)-[:HAS_CREATOR]->(person1:Person)
            OPTIONAL MATCH (message1)<-[:LIKES]-(person2:Person)
            OPTIONAL MATCH (person2)<-[:HAS_CREATOR]-(message2:Message)<-[like:LIKES]-(person3:Person)
            RETURN
                person1.id as id,
                count(DISTINCT like) AS authorityScore
            ORDER BY
                authorityScore DESC,
                id ASC
            LIMIT 100
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

    def benchmark__bi__query_7_analytical(self):
        memgraph = (
            """
            MATCH
                (tag:Tag {name: $tag})<-[:HAS_TAG]-(message:Message),
                (message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(relatedTag:Tag)
            OPTIONAL MATCH (comment)-[:HAS_TAG]->(tag)
            WHERE tag IS NOT NULL
            RETURN
                relatedTag,
                count(DISTINCT comment) AS count
            ORDER BY
                relatedTag.name ASC,
                count DESC
            LIMIT 100
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

        neo4j = (
            """
            MATCH
                (tag:Tag {name: $tag})<-[:HAS_TAG]-(message:Message),
                (message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(relatedTag:Tag)
            WHERE NOT (comment)-[:HAS_TAG]->(tag)
            RETURN
                relatedTag.name,
                count(DISTINCT comment) AS count
            ORDER BY
                relatedTag.name ASC,
                count DESC
            LIMIT 100
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )
        if self._vendor == "memgraph":
            return memgraph
        else:
            return neo4j

    def benchmark__bi__query_9_analytical(self):
        memgraph = (
            """
            MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)<-[:REPLY_OF*0..]-(reply:Message)
            WHERE  post.creationDate >= localDateTime($startDate)
                AND  post.creationDate <= localDateTime($endDate)
                AND reply.creationDate >= localDateTime($startDate)
                AND reply.creationDate <= localDateTime($endDate)
            RETURN
                person.id as id,
                person.firstName,
                person.lastName,
                count(DISTINCT post) AS threadCount,
                count(DISTINCT reply) AS messageCount
            ORDER BY
                messageCount DESC,
                id ASC
            LIMIT 100
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )
        neo4j = (
            """
            MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)<-[:REPLY_OF*0..]-(reply:Message)
            WHERE  post.creationDate >= DateTime($startDate)
                AND  post.creationDate <= DateTime($endDate)
                AND reply.creationDate >= DateTime($startDate)
                AND reply.creationDate <= DateTime($endDate)
            RETURN
                person.id as id,
                person.firstName,
                person.lastName,
                count(DISTINCT post) AS threadCount,
                count(DISTINCT reply) AS messageCount
            ORDER BY
                messageCount DESC,
                id ASC
            LIMIT 100
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )
        if self._vendor == "memgraph":
            return memgraph
        else:
            return neo4j

    def benchmark__bi__query_11_analytical(self):
        return (
            """
            MATCH (a:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country:Country {name: $country}),
                (a)-[k1:KNOWS]-(b:Person)
            WHERE a.id < b.id
                AND localDateTime($startDate) <= k1.creationDate AND k1.creationDate <= localDateTime($endDate)
            WITH DISTINCT country, a, b
            MATCH (b)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
            WITH DISTINCT country, a, b
            MATCH (b)-[k2:KNOWS]-(c:Person),
                (c)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
            WHERE b.id < c.id
                AND localDateTime($startDate) <= k2.creationDate AND k2.creationDate <= localDateTime($endDate)
            WITH DISTINCT a, b, c
            MATCH (c)-[k3:KNOWS]-(a)
            WHERE localDateTime($startDate) <= k3.creationDate AND k3.creationDate <= localDateTime($endDate)
            WITH DISTINCT a, b, c
            RETURN count(*) AS count
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

    def benchmark__bi__query_12_analytical(self):
        return (
            """
            MATCH (person:Person)
            OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(message:Message)-[:REPLY_OF*0..]->(post:Post)
            WHERE message.content IS NOT NULL
                AND message.length < $lengthThreshold
                AND message.creationDate > localDateTime($startDate)
                AND post.language IN $languages
            WITH
                person,
                count(message) AS messageCount
            RETURN
                messageCount,
                count(person) AS personCount
            ORDER BY
                personCount DESC,
                messageCount DESC
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

    def benchmark__bi__query_13_analytical(self):
        memgraph = (
            """
            MATCH (country:Country {name: $country})<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(zombie:Person)
            WHERE zombie.creationDate < localDateTime($endDate)
            WITH country, zombie
            OPTIONAL MATCH (zombie)<-[:HAS_CREATOR]-(message:Message)
            WHERE message.creationDate < localDateTime($endDate)
            WITH
                country,
                zombie,
                count(message) AS messageCount
            WITH
                country,
                zombie,
                12 * (localDateTime($endDate).year  - zombie.creationDate.year )
                    + (localDateTime($endDate).month - zombie.creationDate.month)
                    + 1 AS months,
                messageCount
            WHERE messageCount / months < 1
            WITH
                country,
                collect(zombie) AS zombies
            UNWIND zombies AS zombie
            OPTIONAL MATCH
                (zombie)<-[:HAS_CREATOR]-(message:Message)<-[:LIKES]-(likerZombie:Person)
            WHERE likerZombie IN zombies
            WITH
                zombie,
                count(likerZombie) AS zombieLikeCount
            OPTIONAL MATCH
                (zombie)<-[:HAS_CREATOR]-(message:Message)<-[:LIKES]-(likerPerson:Person)
            WHERE likerPerson.creationDate < localDateTime($endDate)
            WITH
                zombie,
                zombieLikeCount,
                count(likerPerson) AS totalLikeCount
            RETURN
                zombie.id,
                zombieLikeCount,
                totalLikeCount,
            CASE totalLikeCount
            WHEN 0 THEN 0.0
            ELSE zombieLikeCount / toFloat(totalLikeCount)
            END AS zombieScore
            ORDER BY
                zombieScore DESC,
                zombie.id ASC
            LIMIT 100
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

        neo4j = (
            """
            MATCH (country:Country {name: $country})<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(zombie:Person)
            WHERE zombie.creationDate < DateTime($endDate)
            WITH country, zombie
            OPTIONAL MATCH (zombie)<-[:HAS_CREATOR]-(message:Message)
            WHERE message.creationDate < DateTime($endDate)
            WITH
                country,
                zombie,
                count(message) AS messageCount
            WITH
                country,
                zombie,
                12 * (DateTime($endDate).year  - zombie.creationDate.year )
                    + (DateTime($endDate).month - zombie.creationDate.month)
                    + 1 AS months,
                messageCount
            WHERE messageCount / months < 1
            WITH
                country,
                collect(zombie) AS zombies
            UNWIND zombies AS zombie
            OPTIONAL MATCH
                (zombie)<-[:HAS_CREATOR]-(message:Message)<-[:LIKES]-(likerZombie:Person)
            WHERE likerZombie IN zombies
            WITH
                zombie,
                count(likerZombie) AS zombieLikeCount
            OPTIONAL MATCH
                (zombie)<-[:HAS_CREATOR]-(message:Message)<-[:LIKES]-(likerPerson:Person)
            WHERE likerPerson.creationDate < DateTime($endDate)
            WITH
                zombie,
                zombieLikeCount,
                count(likerPerson) AS totalLikeCount
            RETURN
                zombie.id,
                zombieLikeCount,
                totalLikeCount,
            CASE totalLikeCount
            WHEN 0 THEN 0.0
            ELSE zombieLikeCount / toFloat(totalLikeCount)
            END AS zombieScore
            ORDER BY
                zombieScore DESC,
                zombie.id ASC
            LIMIT 100
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

        if self._vendor == "memgraph":
            return memgraph
        else:
            return neo4j

    def benchmark__bi__query_14_analytical(self):
        return (
            """
            MATCH
                (country1:Country {name: $country1})<-[:IS_PART_OF]-(city1:City)<-[:IS_LOCATED_IN]-(person1:Person),
                (country2:Country {name: $country2})<-[:IS_PART_OF]-(city2:City)<-[:IS_LOCATED_IN]-(person2:Person),
                (person1)-[:KNOWS]-(person2)
            WITH person1, person2, city1, 0 AS score
            OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(:Message)-[:HAS_CREATOR]->(person2)
            WITH DISTINCT person1, person2, city1, score + (CASE c WHEN null THEN 0 ELSE  4 END) AS score
            OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(m:Message)<-[:REPLY_OF]-(:Comment)-[:HAS_CREATOR]->(person2)
            WITH DISTINCT person1, person2, city1, score + (CASE m WHEN null THEN 0 ELSE  1 END) AS score
            OPTIONAL MATCH (person1)-[:LIKES]->(m:Message)-[:HAS_CREATOR]->(person2)
            WITH DISTINCT person1, person2, city1, score + (CASE m WHEN null THEN 0 ELSE 10 END) AS score
            OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(m:Message)<-[:LIKES]-(person2)
            WITH DISTINCT person1, person2, city1, score + (CASE m WHEN null THEN 0 ELSE  1 END) AS score
            ORDER BY
                city1.name ASC,
                score DESC,
                person1.id ASC,
                person2.id ASC
            WITH city1, collect({score: score, person1Id: person1.id, person2Id: person2.id})[0] AS top
            RETURN
                top.person1Id,
                top.person2Id,
                city1.name,
                top.score
            ORDER BY
                top.score DESC,
                top.person1Id ASC,
                top.person2Id ASC
            LIMIT 100
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

    def benchmark__bi__query_18_analytical(self):
        memgraph = (
            """
            MATCH (tag:Tag {name: $tag})<-[:HAS_INTEREST]-(person1:Person)-[:KNOWS]-(mutualFriend:Person)-[:KNOWS]-(person2:Person)-[:HAS_INTEREST]->(tag)
            OPTIONAL MATCH (person1)-[:KNOWS]-(person2)
            WHERE person1 <> person2
            RETURN person1.id AS person1Id, person2.id AS person2Id, count(DISTINCT mutualFriend) AS mutualFriendCount
            ORDER BY mutualFriendCount DESC, person1Id ASC, person2Id ASC
            LIMIT 20
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

        neo4j = (
            """
            MATCH (tag:Tag {name: $tag})<-[:HAS_INTEREST]-(person1:Person)-[:KNOWS]-(mutualFriend:Person)-[:KNOWS]-(person2:Person)-[:HAS_INTEREST]->(tag)
            WHERE person1 <> person2
                AND NOT (person1)-[:KNOWS]-(person2)
            RETURN person1.id AS person1Id, person2.id AS person2Id, count(DISTINCT mutualFriend) AS mutualFriendCount
            ORDER BY mutualFriendCount DESC, person1Id ASC, person2Id ASC
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
