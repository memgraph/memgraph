(ns jepsen.memgraph.utils
  (:require
   [neo4j-clj.core :as dbclient]
   [clojure.tools.logging :refer [info]])
  (:import (java.net URI)))

(defn get-instance-url
  "Get Bolt server address for connecting to an instance on a particular port"
  [node port]
  (str "bolt://" node ":" port))

(defn open-bolt
  "Open Bolt connection to the node"
  [node]
  (dbclient/connect (URI. (get-instance-url node 7687)) "" ""))

(defn random-nonempty-subset
  "Return a random nonempty subset of the input collection. Relies on the fact that first 3 instances from the collection are data instances
  and last 3 are coordinators. It kills a random subset of data instances and with 50% probability 1 coordinator."
  [coll]
  (let [data-instances (take 3 coll)
        coords (take-last 3 coll)
        data-instances-to-kill (rand-int (+ 1 (count data-instances)))
        chosen-data-instances (take data-instances-to-kill (shuffle data-instances))
        kill-coord? (< (rand) 0.5)]

    (if kill-coord?
      (let [chosen-coord (first (shuffle coords))
            chosen-instances (conj chosen-data-instances chosen-coord)]
        (info "Chosen instances" chosen-instances)
        chosen-instances)
      (do
        (info "Chosen instances" chosen-data-instances)
        chosen-data-instances))))

; neo4j-clj related utils.
(defmacro with-session
  "Execute body expressions by using the same session. Useful when executing
  multiple queries, each as a separete transaction."
  [connection session & body]
  `(with-open [~session (dbclient/get-session ~connection)]
     ~@body))

(defn op
  "Construct a nemesis op"
  [f]
  {:type :info :f f})
