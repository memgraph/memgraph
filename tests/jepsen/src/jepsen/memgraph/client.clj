(ns jepsen.memgraph.client
  "Neo4j Clojure driver helper functions/macros"
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [jepsen [generator :as gen]])
  (:import (java.net URI)))

;; Jepsen related utils.
(defn instance-url
  "An URL for connecting to an instance on a particular port"
  [node port]
  (str "bolt://" node ":" port))

;; neo4j-clj related utils.
(defmacro with-session
  "Execute body expressions by using the same session. Useful when executing
  multiple queries, each as a separate transaction."
  [connection session & body]
  `(with-open [~session (dbclient/get-session ~connection)]
     ~@body))

(defn open
  "Open client connection to the node"
  [node]
  (dbclient/connect (URI. (instance-url node 7687)) "" ""))

(dbclient/defquery detach-delete-all
  "MATCH (n) DETACH DELETE n;")

(defn replication-mode-str
  [node-config]
  (case (:replication-mode node-config)
    :async "ASYNC"
    :sync  "SYNC" ))

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
  [test process]
  {:type :invoke :f :register :value nil})

(defn replication-gen
  "Generator which should be used for replication tests
  as it adds register replica invoke."
  [generator]
  (gen/each-thread(gen/phases (cycle [(gen/once register-replicas)
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

(defmacro replication-invoke-case
  "Call the case method on the op using the defined cases
  while a handler for :register case is added."
  [f & cases]
  (concat (list 'case f
                :register '(if (= replication-role :main)
                             (do
                               (doseq [n (filter #(= (:replication-role (val %))
                                                     :replica)
                                                 node-config)]
                                                 (try
                                             (c/with-session conn session
                                               ((c/create-register-replica-query
                                                  (first n)
                                                  (second n)) session))
                                                  (catch Exception e)))
                               (assoc op :type :ok))
                             (assoc op :type :fail)))
          cases))
