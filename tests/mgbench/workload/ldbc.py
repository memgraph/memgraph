import inspect
import random
from datetime import datetime
from pathlib import Path

import helpers

from .dataset import Dataset


class LDBC_Interactive(Dataset):
    NAME = "ldbc_interactive"
    VARIANTS = ["sf0.1", "sf1", "sf3", "sf10"]
    DEFAULT_VARIANT = "sf1"

    URL_CYPHER = {
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
        "sf3": {"vertices": 1, "edges": 1},
        "sf10": {"vertices": 1, "edges": 1},
    }

    URL_INDEX_FILES = {
        "memgraph": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/interactive/memgraph_interactive_index.cypher",
        "neo4j": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/interactive/neo4j_interactive_index.cypher",
    }

    PROPERTIES_ON_EDGES = True

    QUERY_PARAMETERS = {
        "sf0.1": "https://repository.surfsara.nl/datasets/cwi/snb/files/substitution_parameters/substitution_parameters-sf0.1.tar.zst",
        "sf1": "https://repository.surfsara.nl/datasets/cwi/snb/files/substitution_parameters/substitution_parameters-sf0.1.tar.zst",
        "sf3": "https://repository.surfsara.nl/datasets/cwi/snb/files/substitution_parameters/substitution_parameters-sf0.1.tar.zst",
    }

    def _prepare_parameters_directory(self):
        parameters = Path() / ".cache" / "datasets" / self.NAME / self._variant / "parameters"
        parameters.mkdir(parents=True, exist_ok=True)
        dir_name = self.QUERY_PARAMETERS[self._variant].split("/")[-1:][0].removesuffix(".tar.zst")
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
            if file.name.split("_")[1] == func_name.split("_")[-1]:
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

    def __init__(self, variant=None, vendor=None):
        super().__init__(variant, vendor)
        self._parameters_dir = self._prepare_parameters_directory()

    def benchmark__interactive__complex_query_1(self):
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

    def benchmark__interactive__complex_query_2(self):
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

    def benchmark__interactive__complex_query_3(self):

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

    def benchmark__interactive__complex_query_4(self):
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

            """,
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

            """,
            self._get_query_parameters(),
        )

        if self._vendor == "memgraph":
            return memgraph
        else:
            return neo4j

    def benchmark__interactive__complex_query_5(self):
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

    def benchmark__interactive__complex_query_6(self):
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

    def benchmark__interactive__complex_query_7(self):
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

    def benchmark__interactive__complex_query_8(self):
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

    def benchmark__interactive__complex_query_9(self):
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

    # def benchmark__interactive__complex_query_10(self):
    #     memgraph = (
    #         """
    #         MATCH (person:Person {id: $personId})-[:KNOWS*2..2]-(friend),
    #             (friend)-[:IS_LOCATED_IN]->(city:City)
    #         WHERE NOT friend=person AND
    #             NOT (friend)-[:KNOWS]-(person)
    #         WITH person, city, friend, datetime({epochMillis: friend.birthday}) as birthday
    #         WHERE  (birthday.month=$month AND birthday.day>=21) OR
    #                 (birthday.month=($month%12)+1 AND birthday.day<22)
    #         WITH DISTINCT friend, city, person
    #         OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
    #         WITH friend, city, collect(post) AS posts, person
    #         WITH friend,
    #             city,
    #             size(posts) AS postCount,
    #             size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)]) AS commonPostCount
    #         RETURN friend.id AS personId,
    #             friend.firstName AS personFirstName,
    #             friend.lastName AS personLastName,
    #             commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
    #             friend.gender AS personGender,
    #             city.name AS personCityName
    #         ORDER BY commonInterestScore DESC, personId ASC
    #         LIMIT 10
    #         """.replace(
    #             "\n", ""
    #         ),
    #         self._get_query_parameters(),
    #     )

    #     neo4j = (
    #         """
    #         MATCH (person:Person {id: $personId})-[:KNOWS*2..2]-(friend),
    #             (friend)-[:IS_LOCATED_IN]->(city:City)
    #         WHERE NOT friend=person AND
    #             NOT (friend)-[:KNOWS]-(person)
    #         WITH person, city, friend, datetime({epochMillis: friend.birthday}) as birthday
    #         WHERE  (birthday.month=$month AND birthday.day>=21) OR
    #                 (birthday.month=($month%12)+1 AND birthday.day<22)
    #         WITH DISTINCT friend, city, person
    #         OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
    #         WITH friend, city, collect(post) AS posts, person
    #         WITH friend,
    #             city,
    #             size(posts) AS postCount,
    #             size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)]) AS commonPostCount
    #         RETURN friend.id AS personId,
    #             friend.firstName AS personFirstName,
    #             friend.lastName AS personLastName,
    #             commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
    #             friend.gender AS personGender,
    #             city.name AS personCityName
    #         ORDER BY commonInterestScore DESC, personId ASC
    #         LIMIT 10
    #         """.replace(
    #             "\n", ""
    #         ),
    #         self._get_query_parameters(),
    #     )

    #     if self._vendor == "memgraph":
    #         return memgraph
    #     else:
    #         return neo4j

    def benchmark__interactive__complex_query_11(self):
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

    def benchmark__interactive__complex_query_12(self):
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

    def benchmark__interactive__complex_query_13(self):
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


class LDBC_BI(Dataset):
    NAME = "ldbc_bi"
    VARIANTS = ["sf1", "sf3", "sf10"]
    DEFAULT_VARIANT = "sf1"

    URL_CYPHER = {
        "sf1": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/bi/ldbc_bi_sf1.cypher.gz",
        "sf3": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/bi/ldbc_bi_sf3.cypher.gz",
        "sf10": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/bi/ldbc_bi_sf10.cypher.gz",
    }

    URL_CSV = {
        "sf1": "https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/bi-sf1-composite-projected-fk.tar.zst",
        "sf3": "https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/bi-sf3-composite-projected-fk.tar.zst",
        "sf10": "https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/bi-sf10-composite-projected-fk.tar.zst",
    }

    # 2997352 15457338
    SIZES = {
        "sf1": {"vertices": 2997352, "edges": 17196776},
        "sf3": {"vertices": 1, "edges": 1},
        "sf10": {"vertices": 1, "edges": 1},
    }

    LOCAL_INDEX_FILES = None

    URL_INDEX_FILES = {
        "memgraph": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/bi/memgraph_bi_index.cypher",
        "neo4j": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/bi/neo4j_bi_index.cypher",
    }

    QUERY_PARAMETERS = {
        "sf1": "https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/parameters-2022-10-01.zip",
        "sf3": "https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/parameters-2022-10-01.zip",
        "sf10": "https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/parameters-2022-10-01.zip",
    }

    def _prepare_parameters_directory(self):
        parameters = Path() / ".cache" / "datasets" / self.NAME / self._variant / "parameters"
        parameters.mkdir(parents=True, exist_ok=True)
        if parameters.exists() and any(parameters.iterdir()):
            print("Files downloaded.")
        else:
            print("Downloading files")
            downloaded_file = helpers.download_file(self.QUERY_PARAMETERS[self._variant], parameters.parent.absolute())
            print("Unpacking the file..." + downloaded_file)
            parameters = helpers.unpack_zip(Path(downloaded_file))
        return parameters / ("parameters-" + self._variant)

    def _get_query_parameters(self) -> dict:
        func_name = inspect.stack()[1].function
        parameters = {}
        for file in self._parameters_dir.glob("bi-*.csv"):
            file_name_query_id = file.name.split("-")[1][0:-4]
            func_name_id = func_name.split("_")[-1]
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

    def __init__(self, variant=None, vendor=None):
        super().__init__(variant, vendor)
        self._parameters_dir = self._prepare_parameters_directory()

    def benchmark__bi__query_1(self):

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

    def benchmark__bi__query_2(self):

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

    def benchmark__bi__query_3(self):
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

    def benchmark__bi__query_5(self):
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

    def benchmark__bi__query_6(self):
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

    def benchmark__bi__query_7(self):

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

    def benchmark__bi__query_9(self):
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

    def benchmark__bi__query_11(self):
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

    def benchmark__bi__query_12(self):
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

    def benchmark__bi__query_13(self):
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

    def benchmark__bi__query_14(self):
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

    def benchmark__bi__query_17(self):

        memgraph = (
            """
            MATCH
                (tag:Tag {name: $tag}),
                (person1:Person)<-[:HAS_CREATOR]-(message1:Message)-[:REPLY_OF*0..]->(post1:Post)<-[:CONTAINER_OF]-(forum1:Forum),
                (message1)-[:HAS_TAG]->(tag),
                (forum1)<-[:HAS_MEMBER]->(person2:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:HAS_TAG]->(tag),
                (forum1)<-[:HAS_MEMBER]->(person3:Person)<-[:HAS_CREATOR]-(message2:Message),
                (comment)-[:REPLY_OF]->(message2)-[:REPLY_OF*0..]->(post2:Post)<-[:CONTAINER_OF]-(forum2:Forum)
            MATCH (comment)-[:HAS_TAG]->(tag)
            MATCH (message2)-[:HAS_TAG]->(tag)
            OPTIONAL MATCH (forum2)-[:HAS_MEMBER]->(person1)
            WHERE forum1 <> forum2 AND message2.creationDate > message1.creationDate + duration({hours: $delta}) AND person1 IS NULL
            RETURN person1, count(DISTINCT message2) AS messageCount
            ORDER BY messageCount DESC, person1.id ASC
            LIMIT 10
            """.replace(
                "\n", ""
            ),
            self._get_query_parameters(),
        )

        neo4j = (
            """
            MATCH
                (tag:Tag {name: $tag}),
                (person1:Person)<-[:HAS_CREATOR]-(message1:Message)-[:REPLY_OF*0..]->(post1:Post)<-[:CONTAINER_OF]-(forum1:Forum),
                (message1)-[:HAS_TAG]->(tag),
                (forum1)<-[:HAS_MEMBER]->(person2:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:HAS_TAG]->(tag),
                (forum1)<-[:HAS_MEMBER]->(person3:Person)<-[:HAS_CREATOR]-(message2:Message),
                (comment)-[:REPLY_OF]->(message2)-[:REPLY_OF*0..]->(post2:Post)<-[:CONTAINER_OF]-(forum2:Forum)
            MATCH (comment)-[:HAS_TAG]->(tag)
            MATCH (message2)-[:HAS_TAG]->(tag)
            WHERE forum1 <> forum2
                AND message2.creationDate > message1.creationDate + duration({hours: $delta})
                AND NOT (forum2)-[:HAS_MEMBER]->(person1)
            RETURN person1, count(DISTINCT message2) AS messageCount
            ORDER BY messageCount DESC, person1.id ASC
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

    def benchmark__bi__query_18(self):

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
