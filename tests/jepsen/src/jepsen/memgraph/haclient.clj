(ns jepsen.memgraph.haclient
  "Neo4j Clojure driver helper functions/macros"
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [jepsen [generator :as gen]]))

(defn register-replication-instance
  [name node-config]
  (dbclient/create-query
   (let [query
         (str "REGISTER INSTANCE "
              name
              " WITH CONFIG {'bolt_server': '"
              (:ip node-config)
              ":7687', "
              "'management_server': '"
              (:ip node-config)
              ":" (str (:management-port node-config)) "', "
              "'replication_server': '"
              (:ip node-config)
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
  [node-config]
  (dbclient/create-query
   (let [query
         (str "ADD COORDINATOR "
              (str (:coordinator-id node-config))
              " WITH CONFIG {'bolt_server': '"
              (:ip node-config)
              ":7687', "
              "'coordinator_server': '"
              (:ip node-config)
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
  (gen/each-thread (gen/phases (cycle [(gen/once register-replication-instances)
                                       (gen/time-limit 5 generator)]))))
