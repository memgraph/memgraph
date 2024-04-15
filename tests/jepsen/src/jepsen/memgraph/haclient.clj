(ns jepsen.memgraph.haclient
  "Neo4j Clojure driver helper functions/macros"
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [jepsen [generator :as gen]
             [client :as client]]
            [jepsen.memgraph.utils :as utils]))

(defn register-replication-instance
  [name node-config]
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

(defn add-coordinator-instance
  [name node-config]
  (dbclient/create-query
   (let [query
         (str "ADD COORDINATOR "
              (str (:coordinator-id node-config))
              " WITH CONFIG {'bolt_server': '"
              name
              ":7687', "
              "'coordinator_server': '"
              name
              ":" (str (:coordinator-port node-config)) "'}")]
     (info "Adding coordinator instance" query)
     query)))

(defn register-replication-instances
  "Register all replication instances."
  [_ _]
  {:type :invoke :f :register :value nil})

(defn ha-gen
  "Generator which should be used for HA tests
  as it adds register replication instance invoke."
  [generator]
  (gen/each-thread (gen/phases [(gen/once register-replication-instances)
                                (gen/time-limit 5 generator)])))

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
