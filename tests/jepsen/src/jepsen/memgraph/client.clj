(ns jepsen.memgraph.client
  "Neo4j Clojure driver helper functions/macros"
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [jepsen [generator :as gen]
             [client :as client]]
            [jepsen.memgraph.utils :as utils]))

(dbclient/defquery detach-delete-all
  "MATCH (n) DETACH DELETE n;")

(defn replication-mode-str
  [node-config]
  (case (:replication-mode node-config)
    :async "ASYNC"
    :sync  "SYNC"))

; Used by replication-invoke-case
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

; TODO: (andi) Why is this function part of generator?
(defn register-replicas
  "Register all replicas."
  [_ _]
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
  (concat `(defrecord ~name [~'conn ~'node ~'replication-role ~'nodes-config ~@fields]
             client/Client)
          specs))

(defn replication-open-connection
  "Open a connection to a node using the client.
  After the connection is opened set the correct
  replication role of instance."
  [client node nodes-config]
  (let [connection (utils/open node)
        node-config (get nodes-config node)
        role (:replication-role node-config)]
    (when (= :replica role)
      (utils/with-session connection session
        (try
          ((create-set-replica-role-query (:port node-config)) session)
          (catch Exception _
            (info "The role is already setup")))))

    (assoc client
           :replication-role role
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
                                                 nodes-config)]
                                 (try
                                   (utils/with-session conn session
                                     ((client/create-register-replica-query
                                       (first n)
                                       (second n)) session))
                                   (catch Exception e)))
                               (assoc op :type :ok))
                             (assoc op :type :fail)))
          cases))
