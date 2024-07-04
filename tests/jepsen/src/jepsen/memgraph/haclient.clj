(ns jepsen.memgraph.haclient
  "Neo4j Clojure driver helper functions/macros"
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]))

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
