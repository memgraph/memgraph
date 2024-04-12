(ns jepsen.memgraph.utils
  (:require [clojure.string :as str]
            [neo4j-clj.core :as dbclient])
  (:import (java.net URI)))

(defn get-instance-url
  "Get Bolt server address for connecting to an instance on a particular port"
  [node port]
  (str "bolt://" node ":" port))

(defn open-bolt
  "Open Bolt connection to the node"
  [node]
  (dbclient/connect (URI. (get-instance-url node 7687)) "" ""))

(defn expected-expection?
  "Check if the exception is expected."
  [exception-message expected-message]
  (str/includes? exception-message expected-message))

(defn rethrow-if-unexpected
  [exception expected-message]
  (when-not (expected-expection? (str exception) expected-message)
    (throw (Exception. (str "Invalid exception happened: " exception)))))

; neo4j-clj related utils.
(defmacro with-session
  "Execute body expressions by using the same session. Useful when executing
  multiple queries, each as a separete transaction."
  [connection session & body]
  `(with-open [~session (dbclient/get-session ~connection)]
     ~@body))
