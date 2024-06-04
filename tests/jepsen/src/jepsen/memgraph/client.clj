(ns jepsen.memgraph.client
  "Neo4j Clojure driver helper functions/macros"
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [jepsen [generator :as gen]]
            [jepsen.memgraph.utils :as utils]))

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

(dbclient/defquery get-all-instances
  "SHOW INSTANCES;")

(dbclient/defquery show-repl-role
  "SHOW REPLICATION ROLE")

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
        (:ip node-config)
        ":"
        (:port node-config)
        "'")))

(defn create-set-replica-role-query
  [port]
  (dbclient/create-query
   (str "SET REPLICATION ROLE TO REPLICA WITH PORT " port)))

(defn register-replicas
  "Register all replicas."
  [_ _]
  {:type :invoke :f :register :value nil})

(defn replication-gen
  "Generator which should be used for replication tests
  as it adds register replica invoke."
  [generator]
  (gen/each-thread (gen/phases (cycle [(gen/once register-replicas)
                                       (gen/time-limit 5 generator)]))))

(defn replication-open-connection
  "Open a connection to a node using the client.
  After the connection is opened set the correct
  replication role of instance."
  [client node nodes-config]
  (let [connection (utils/open-bolt node)
        node-config (get nodes-config node)
        role (:replication-role node-config)]
    (when (= :replica role)
      (utils/with-session connection session
        (try
          ((create-set-replica-role-query (:port node-config)) session)
          (catch Exception _
            (info "The role is already setup")))))

    (assoc client
           :replication-role role
           :conn connection
           :node node)))
