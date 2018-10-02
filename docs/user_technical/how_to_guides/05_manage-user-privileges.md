## How to Manage User Privileges?

Most databases have multiple users accessing and modifying
data within the database. This might pose a serious security concern for the
system administrators that wish to grant only certain privileges to certain
users. A typical example would be an internal database of some company which
tracks data about their employees. Naturally, only certain users of the database
should be able to perform queries which modify that data.

At Memgraph, we provide the administrators with the option of granting,
denying or revoking a certain set of privileges to some users or groups of users
(i.e. users that are assigned a specific user role), thereby eliminating such
security concerns.

By default, anyone can connect to Memgraph and is granted all privileges.
After the first user is created, Memgraph will execute a query if and only
if either a user or its role is granted that privilege and neither the
user nor its role are denied that privilege. Otherwise, Memgraph will not
execute that specific query. Note that `DENY` is a stronger
operation than `GRANT`. This is also notable from the fact that if neither the
user nor its role are explicitly granted or denied a certain privilege, that
user will not be able to perform that specific query. This effect also is known
as a silent deny. The information above is neatly condensed in the following
table:

User Status | Role Status | Effective Status
---------------------------------------------
GRANT       | GRANT       | GRANT
GRANT       | DENY        | DENY
GRANT       | NULL        | GRANT
DENY        | GRANT       | DENY
DENY        | DENY        | DENY
DENY        | NULL        | DENY
NULL        | GRANT       | GRANT
NULL        | DENY        | DENY
NULL        | NULL        | DENY

All supported commands that deal with accessing or modifying users, user
roles and privileges can only be executed by users that are granted the
`AUTH` privilege. All of those commands are listed in the appropriate
[reference guide](../reference_guide/security.md).

At the moment, privileges are confined to users' abilities to perform certain
`OpenCypher` queries. Namely users can be given permission to execute a subset
of the following commands: `CREATE`, `DELETE`, `MATCH`, `MERGE`, `SET`,
`REMOVE`, `INDEX`, `AUTH`, `STREAM`.

We could naturally cluster those privileges into groups:

  * Privilege to access data (`MATCH`)
  * Privilege to modify data (`MERGE`, `SET`)
  * Privilege to create and delete data (`CREATE`, `DELETE`, `REMOVE`)
  * Privilege to index data (`INDEX`)
  * Privilege to use data streaming (`STREAM`)
  * Privilege to view and alter users, roles and privileges (`AUTH`)

If you are unfamiliar with any of these commands, you can look them up in our
[reference guide](../reference_guide/01_reference-overview.md).

Similarly, the complete list of commands which can be executed under `AUTH`
privilege can be viewed in the
[appropriate article](../reference_guide/08_security.md) within  our reference
guide.

The remainder of this article outlines a recommended workflow of
user management within an internal database of a fictitious company.

### Creating an Administrator

As it was stated in the introduction, after the first user is created, Memgraph
will execute a query for a given user if the effective status of a corresponding
privilege evaluates to `GRANT`. As a corollary, the person that created the
first user might not be able to perform any meaningful action after their
session had ended. To prevent that from happening, we strongly recommend
the first created user to be an administrator which is granted all privileges.

Therefore, let's create a user named `admin` and set its' password to `0000`.
This can be done by executing:

```openCypher
  CREATE USER admin IDENTIFIED BY '0000';
```

Granting all privileges to our `admin` user can be done as follows:

```openCypher
  GRANT ALL PRIVILEGES to admin;
```

At this point, the current user can close their session and log into a new
one as an `admin` user they have just created. The remainder of the article
is written from the viewpoint of an administrator which is granted
all privileges.

### Creating Other Users

Our fictitious company is internally divided into teams, and each team has
its own supervisor. All employees of the company need to access and modify
data within the database.

Creating a user account for a new hire named Alice can be done as follows:

```openCypher
  CREATE USER alice IDENTIFIED BY '0042';
```

Alice should also be granted a privilege to access data, which can be done by
executing the following:

```openCypher
  GRANT MATCH, MERGE, SET TO alice;
```

### Creating User Roles

Each team supervisor needs to have additional privileges that allow them to
create new data or delete existing data from the database. Instead of tediously
granting additional privileges to each supervisor using language constructs from
the previous chapter, we could do so by creating a new user role for
supervisors.

Creating a user role named `supervisor` can be done by executing the following
command:

```openCypher
  CREATE ROLE supervisor;
```

Granting the privilege to create and delete data to our newly created role can
be done as follows:

```openCypher
  GRANT CREATE, DELETE, REMOVE TO supervisor;
```

Finally, we need to assign that role to each of the supervisors. Suppose, a user
named `bob` is indeed a supervisor within the company. Assigning them that role
within the database can be done by the following command:

```
  SET ROLE FOR bob TO supervisor;
```
