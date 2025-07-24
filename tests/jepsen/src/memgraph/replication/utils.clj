(ns memgraph.replication.utils
  "Neo4j Clojure driver helper functions/macros"
  (:require
   [clojure.tools.logging :refer [info]]
   [jepsen [generator :as gen]]
   [memgraph.utils :as utils]
   [memgraph.query :as query]))

(defn register-replicas
  "Register all replicas."
  [_ _]
  {:type :invoke :f :register :value nil})

(defn replication-gen
  "Generator which should be used for replication tests
  as it adds register replica invoke."
  [ops]
  (gen/each-thread
    (gen/phases
      (gen/once register-replicas)
      (gen/sleep 5)
      (gen/delay 3 (gen/mix ops) )))


(defn replication-open-connection
  "Open a connection to a node using the client.
  After the connection is opened set the correct
  replication role of instance."
  [client node nodes-config]
  (let [connection (utils/open-bolt node)
        node-config (get nodes-config node)
        role (:replication-role node-config)]
    (when (= :replica role)
      (utils/with-session connection session
        (try
          ((query/create-set-replica-role-query (:port node-config)) session)
          (catch Exception _
            (info "The role is already setup")))))

    (assoc client
           :replication-role role
           :conn connection
           :node node)))
