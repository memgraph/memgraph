(ns memgraph.query
  "Neo4j Clojure driver helper functions/macros"
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]))

(dbclient/defquery get-all-instances
  "SHOW INSTANCES;")


(dbclient/defquery show-replication-role
  "SHOW REPLICATION ROLE;")


(dbclient/defquery detach-delete-all
  "MATCH (n) DETACH DELETE n;")

; Implicit 1st parameter you need to send is txn. 2nd is id. 3rd balance
(dbclient/defquery create-account
  "CREATE (n:Account {id: $id, balance: $balance});")

; Implicit 1st parameter you need to send is txn.
(dbclient/defquery get-all-accounts
  "MATCH (n:Account) RETURN n;")

; Implicit 1st parameter you need to send is txn. 2nd is id.
(dbclient/defquery get-account
  "MATCH (n:Account {id: $id}) RETURN n;")

; Implicit 1st parameter you need to send is txn. 2nd is id. 3d is amount.
(dbclient/defquery update-balance
  "MATCH (n:Account {id: $id})
   SET n.balance = n.balance + $amount
   RETURN n")

(dbclient/defquery collect-ids
  "MATCH (n:Node)
  RETURN n.id as id;
  ")

(dbclient/defquery max-id
  "MATCH (n:Node)
  RETURN max(n.id) as id;
  ")

(defn add-nodes
  [start-idx end-idx]
  (dbclient/create-query
   (str "FOREACH (i in range(" start-idx ", " end-idx ") | CREATE (:Node {id: i}));")))

(defn register-replication-instance
  [name node-config]
  (info "name" name "node-config" node-config)
  (dbclient/create-query
   (let [query
         (str "REGISTER INSTANCE "
              name
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
