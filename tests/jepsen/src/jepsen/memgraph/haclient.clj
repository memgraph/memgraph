(ns jepsen.memgraph.haclient
  "Neo4j Clojure driver helper functions/macros"
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [jepsen [generator :as gen]
             [client :as client]]
            [jepsen.memgraph.utils :as utils])
  (:import (java.net URI)))

;; neo4j-clj related utils.
(defmacro with-session
  "Execute body expressions by using the same session. Useful when executing
  multiple queries, each as a separete transaction."
  [connection session & body]
  `(with-open [~session (dbclient/get-session ~connection)]
     ~@body))

(defn open
  "Open client connection to the node"
  [node]
  (dbclient/connect (URI. (utils/get-instance-url node 7687)) "" ""))

(dbclient/defquery detach-delete-all
  "MATCH (n) DETACH DELETE n;")

(defn replication-mode-str
  [node-config]
  (case (:replication-mode node-config)
    :async "ASYNC"
    :sync  "SYNC"))

(defn create-set-replica-role-query
  [port]
  (dbclient/create-query
   (str "SET REPLICATION ROLE TO REPLICA WITH PORT " port)))

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

(defn register-replication-instance
  [instance-name management-port replication-port]
  (info "Bolt server for instance: " instance-name "is: " (utils/get-registration-url instance-name 7687))
  (info "Management server for instance: " instance-name "is: " (utils/get-registration-url instance-name management-port))
  (info "Replication server for instance: " instance-name "is " (utils/get-registration-url instance-name replication-port))
  (dbclient/create-query
   (str "REGISTER INSTANCE " instance-name "WITH CONFIG {'bolt_server':" (utils/get-registration-url instance-name 7687)
        ", 'management_server':" (utils/get-registration-url instance-name management-port) "'replication_server':" (utils/get-registration-url instance-name replication-port) "}")))

; TODO: (andi) Why is this function part of generator?
(defn register-replicas
  "Register all replicas."
  []
  {:type :invoke :f :register :value nil})

(defn replication-gen
  "Generator which should be used for replication tests
  as it adds register replica invoke."
  [generator]
  (gen/each-thread (gen/phases (cycle [(gen/once register-replicas)
                                       (gen/time-limit 5 generator)]))))

(defmacro replication-client
  "Create Client for replication tests.
  Every replication client contains connection, node, replication role and
  the node config for all nodes.
  Adding additional fields is also possible."
  [name [& fields] & specs]
  (concat `(defrecord ~name [~'conn ~'node ~'replication-role ~'node-config ~@fields]
             client/Client)
          specs))

(defn register-replication-instances
  "Register replication instances on coordinator with ID 1"
  [client node node-configs]
  (let [connection (open node)
        node-config (get node-configs node)
        coordinator-id (:coordinator-id node-config)]
    (when (= coordinator-id 1)
      (with-session connection session))))

(defn replication-open-connection
  "Open a connection to a node using the client.
  After the connection is opened set the correct
  replication role of instance."
  [client node node-config]
  (let [connection (open node)
        nc (get node-config node)
        role (:replication-role nc)]
    (when (= :replica role)
      (with-session connection session
        (try
          ((create-set-replica-role-query (:port nc)) session)
          (catch Exception e
            (info "Already setup the role")))))

    (assoc client :replication-role role
           :conn connection
           :node node)))
