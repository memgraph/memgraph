(ns memgraph.replication.utils
  "Neo4j Clojure driver helper functions/macros"
  (:require
   [clojure.set]
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
      (gen/sleep 10)
      (gen/once register-replicas)
      (gen/sleep 5)
      (gen/delay 3 (gen/mix ops)))))


(defn replica-nodes
  "Names of all nodes configured as replicas."
  [nodes-config]
  (->> nodes-config
       (filter #(= :replica (:replication-role (val %))))
       (map key)
       (set)))

(defn- is-replica?
  "Ask the node which replication role it currently holds."
  [connection]
  (try
    (utils/with-session connection session
      (let [role-map (first (reduce conj [] (query/show-replication-role session)))
            role (last (vec (apply concat role-map)))]
        (= role "replica")))
    (catch Exception _
      false)))

(defn- set-replica-role!
  "Set the replica role and confirm it took effect. Returns true on success."
  [connection port]
  (utils/with-session connection session
    (try
      ((query/create-set-replica-role-query port) session)
      (catch Exception _
        (info "The role is already setup"))))
  (is-replica? connection))

(defn replication-open-connection
  "Open a connection to a node using the client.
  After the connection is opened set the correct
  replication role of instance. Replicas that confirm
  their role are added to the shared replicas-ready set
  so main knows when it may start registering them."
  [client node nodes-config replicas-ready]
  (let [connection (utils/open-bolt node)
        node-config (get nodes-config node)
        role (:replication-role node-config)]
    (when (= :replica role)
      (if (set-replica-role! connection (:port node-config))
        (do
          (swap! replicas-ready conj node)
          (info "Node" node "is ready as replica"))
        (info "Node" node "did not confirm the replica role")))

    (assoc client
           :replication-role role
           :conn connection
           :node node)))

(def replicas-ready-timeout-ms
  "How long main waits for every replica to confirm its role before registering."
  120000)

(defn wait-for-replicas
  "Block until every configured replica has confirmed its role or the timeout
  expires. Returns the set of replicas that were still not ready."
  [nodes-config replicas-ready]
  (let [expected (replica-nodes nodes-config)
        deadline (+ (System/currentTimeMillis) replicas-ready-timeout-ms)]
    (loop []
      (let [missing (clojure.set/difference expected @replicas-ready)]
        (cond
          (empty? missing) missing
          (> (System/currentTimeMillis) deadline)
          (do
            (info "Timed out waiting for replicas" missing "to confirm their role")
            missing)
          :else
          (do
            (Thread/sleep 1000)
            (recur)))))))
