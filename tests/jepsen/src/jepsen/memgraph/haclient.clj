(ns jepsen.memgraph.haclient
  "Neo4j Clojure driver helper functions/macros"
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [jepsen [generator :as gen]
             [client :as client]]
            [jepsen.memgraph.utils :as utils]))

(defn replication-gen
  "Generator which should be used for replication tests
  as it adds register replica invoke."
  [generator]
  (info "Replication generator called")
  (gen/each-thread (gen/phases (cycle [(gen/time-limit 5 generator)]))))

(defmacro ha-client
  "Create Client for HA tests."
  [name [& fields] & specs]
  (info "Ha client created")
  (concat `(defrecord ~name [~'conn ~'node ~'node-config ~'nodes-config ~@fields]
             client/Client)
          specs))

; TODO: (andi) Maybe move it to haempty.clj test if haclient won't make sense
(defn ha-open-connection
  "Open a connection to a node using the client."
  [client node nodes-config]
  (info "Opening connection to node" node)
  (let [connection (utils/open-bolt node)
        node-config (get nodes-config node)]
    (assoc client
           :conn connection
           :node-config node-config
           :node node)))

(defmacro ha-invoke-case
  "Call the case method on the op using the defined cases
  while a handler for :register case is added."
  [f & cases]
  (concat (list 'case f
                :register '(assoc op :type :ok))
          cases))
