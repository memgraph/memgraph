(ns memgraph.query
  "Neo4j Clojure driver helper functions/macros"
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery update-pokec-nodes
  "unwind range($param, $param + 100) as x
  match (n:User {id: x})
  set n.prop = {int_value: 1, double_value: 2.2, string_value: 'little_string', list_value: [1, 2, 3], map_value: {nested_obj: {value: 1, str_value: 'nested_prop'}}, date_value: date(), datetime_value: localdatetime()};
  ")

#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery create-ttl-edges
  "unwind range($param, $param + 100) as x
  match (n:User {id: x}) match (n:User {id: x + 100}) CREATE (n)-[:KNOWS {ttl: timestamp() + timestamp(duration({minute: 1})) }]->(m);
  ")

#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery delete-ttl-edges
  "MATCH (n)-[r:KNOWS]->(m) where r.ttl < timestamp() WITH r LIMIT 1000 DELETE r;
  ")

(defn create-database
  "Creates DB with name 'db'."
  [db]
  (dbclient/create-query
   (let [query (str "CREATE DATABASE " db)]
     query)))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery create-ttl-edge-idx
  "create edge index on :KNOWS(ttl);
  ")

#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery create-label-idx
  "
  CREATE INDEX ON :User;
  ")

#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery create-label-property-idx
  "
  CREATE INDEX ON :User(id);
  ")

; Path inside the container
#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery import-pokec-medium-nodes
  "
  LOAD CSV FROM '/opt/memgraph/datasets/nodes.csv' WITH HEADER AS row
  CREATE (:User {id: row.id});
  ")

; Path inside the container
#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery import-pokec-medium-edges
  "
  LOAD CSV FROM '/opt/memgraph/datasets/relationships.csv' WITH HEADER AS row
  MATCH (n1:User {id: row.from_id})
  MATCH (n2:User {id: row.to_id})
  CREATE (n1)-[:KNOWS]->(n2);
  ")

#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery get-num-nodes
  "
  MATCH (n) RETURN count(n) as c;
  ")

#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery get-num-edges
  "
  MATCH (n)-[e]->(m) RETURN count(e) as c;
  ")

#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery get-all-instances
  "SHOW INSTANCES;")

#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery show-replication-role
  "SHOW REPLICATION ROLE;")

#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery detach-delete-all
  "MATCH (n) DETACH DELETE n;")

; Implicit 1st parameter you need to send is txn. 2nd is id. 3rd balance
#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery create-account
  "CREATE (n:Account {id: $id, balance: $balance});")

; Implicit 1st parameter you need to send is txn.
#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery get-all-accounts
  "MATCH (n:Account) RETURN n;")

; Implicit 1st parameter you need to send is txn. 2nd is id.
#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery get-account
  "MATCH (n:Account {id: $id}) RETURN n;")

; Implicit 1st parameter you need to send is txn. 2nd is id. 3d is amount.
#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery update-balance
  "MATCH (n:Account {id: $id})
   SET n.balance = n.balance + $amount
   RETURN n")

#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery collect-ids
  "MATCH (n:Node)
  RETURN n.id as id;
  ")

#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery create-unique-constraint
  "
  CREATE CONSTRAINT ON (n:Node) ASSERT n.id IS UNIQUE;
  ")

#_{:clj-kondo/ignore [:unresolved-symbol]}
(dbclient/defquery add-nodes
  "MATCH (n:Node)
  WITH coalesce(max(n.id), 0) as max_idx
  FOREACH (i in range(max_idx + 1, max_idx + $batchSize)
    | CREATE (:Node {id: i}))
  RETURN max_idx + $batchSize as id;
  ")

(defn register-replication-instance
  [name node-config]
  (info "name" name "node-config" node-config)
  (dbclient/create-query
   (let [maybe-suffix
         (cond
           (= "async" (:replica-type node-config)) " AS ASYNC"
           (= "strict_sync" (:replica-type node-config)) " AS STRICT_SYNC"
           :else "")

         query
         (str "REGISTER INSTANCE "
              name
              maybe-suffix
              " WITH CONFIG {'bolt_server': '"
              name
              ":7687', "
              "'management_server': '"
              name
              ":" (str (:management-port node-config)) "', "
              "'replication_server': '"
              name
              ":" (str (:replication-port node-config)) "'}")]
     (info "Registering replication instance" query)
     query)))

(defn set-db-setting
  [setting value]
  (dbclient/create-query
   (let [query
         (str "SET DATABASE SETTING '"
              setting
              "' TO '"
              value
              "'")]
     query)))

(defn set-instance-to-main
  [name]
  (dbclient/create-query
   (let [query
         (str "SET INSTANCE "
              name
              " TO MAIN")]

     (info "Setting instance to main" query)
     query)))

(defn add-coordinator-instance
  [name node-config]
  (info "Name" name "Node config" node-config)
  (dbclient/create-query
   (let [query
         (str "ADD COORDINATOR "
              (str (:coordinator-id node-config))
              " WITH CONFIG {'bolt_server': '"
              name
              ":7687', "
              "'coordinator_server': '"
              name
              ":" (str (:coordinator-port node-config)) "', "
              "'management_server': '"
              name
              ":" (str (:management-port node-config))

              "'}")]
     (info "Adding coordinator instance" query)
     query)))

(defn replication-mode-str
  [node-config]
  (case (:replication-mode node-config)
    :async "ASYNC"
    :sync  "SYNC"))

; Used by replication-invoke-case
(defn create-register-replica-query
  [name node-config]
  (dbclient/create-query
   (str "REGISTER REPLICA "
        name
        " "
        (replication-mode-str node-config)
        " TO '"
        name
        ":"
        (:port node-config)
        "'")))

(defn create-set-replica-role-query
  [port]
  (dbclient/create-query
   (str "SET REPLICATION ROLE TO REPLICA WITH PORT " port)))
