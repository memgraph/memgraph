(ns jepsen.memgraph.client
  "Neo4j Clojure driver helper functions/macros"
  (:require [neo4j-clj.core :as dbclient])
  (:import (java.net URI)))

(ns neo4j-clj.core
  (:import (java.util.concurrent TimeUnit)))
(defn config [options]
  (let [logging (:logging options (ConsoleLogging. Level/CONFIG))
        timeunit TimeUnit/SECONDS]
    (-> (Config/builder)
        (.withLogging logging)
        (.build))))
(ns jepsen.memgraph.client)

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
