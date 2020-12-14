(ns jepsen.memgraph.client
  "Neo4j Clojure driver helper functions/macros"
  (:require [neo4j-clj.core :as dbclient])
  (:import (java.net URI)))

;; Jepsen related utils.
(defn instance-url
  "An URL for connecting to an instance on a particular port"
  [node port]
  (str "bolt://" node ":" port))

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
  (dbclient/connect (URI. (instance-url node 7687)) "" ""))

(dbclient/defquery detach-delete-all
  "MATCH (n) DETACH DELETE n;")

(defn replica-mode-str
  [node-config]
  (case (:replication-mode node-config)
    :async "ASYNC"
    :sync  (str "SYNC" (when-let [timeout (:timeout node-config)] (str " WITH TIMEOUT " timeout)))))

(defn create-register-replica-query
  [name node-config]
  (dbclient/create-query
    (str "REGISTER REPLICA "
         name
         " "
         (replica-mode-str node-config)
         " TO \""
         (:ip node-config)
         ":"
         (:port node-config)
         "\"")))

(defn create-set-replica-role-query
  [port]
  (dbclient/create-query
    (str "SET REPLICATION ROLE TO REPLICA WITH PORT " port)))

