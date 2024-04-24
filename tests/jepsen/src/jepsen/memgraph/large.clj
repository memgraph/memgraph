(ns jepsen.memgraph.large
  "Large write test"
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as string]
            [jepsen
             [checker :as checker]
             [client :as client]
             [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.memgraph.utils :as utils]
            [jepsen.memgraph.client :as mgclient]))

; It is important that at least once applying deltas passes to replicas. Before this value was 100k so the instance never had
; enough time to apply all deltas.
(def node-num 5000)

(dbclient/defquery get-node-count
  "MATCH (n:Node) RETURN count(n) as c;")

(defn create-nodes-builder
  []
  (dbclient/create-query
   (str "UNWIND range(1, " node-num ") AS i CREATE (n:Node {id: i, property1: 0, property2: 1, property3: 2});")))

(def create-nodes (create-nodes-builder))

(defrecord Client [nodes-config]
  client/Client
  ; Open connection to the node. Setup each node.
  (open! [this _test node]
    (mgclient/replication-open-connection this node nodes-config))
  ; On main detach-delete-all and create-nodes.
  (setup! [this _test]
    (when (= (:replication-role this) :main)
      (utils/with-session (:conn this) session
        (mgclient/detach-delete-all session)
        (create-nodes session)
        (info "Initial nodes created."))))
  (invoke! [this _test op]
    (case (:f op)
      ; Create a map with the following structure: {:type :ok, :value {:count count, :node node}}
      :read   (utils/with-session (:conn this) session
                (assoc op
                       :type :ok
                       :value {:count (->> (get-node-count session)
                                           first
                                           :c)
                               :node (:node this)}))

      :register (if (= (:replication-role this) :main)
                  (do
                    (doseq [n (filter #(= (:replication-role (val %))
                                          :replica)
                                      nodes-config)]
                      (try
                        (utils/with-session (:conn this) session
                          ((mgclient/create-register-replica-query
                            (first n)
                            (second n)) session))
                        (catch Exception _e)))
                    (assoc op :type :ok))
                  (assoc op :type :fail))

      ; When executed on main, create nodes.
      :add    (if (= (:replication-role this) :main)
                (utils/with-session (:conn this) session
                  (try
                    ((create-nodes session)
                     (assoc op :type :ok :value "Nodes created."))
                    (catch Exception e
                      (if (string/includes? (str e) "At least one SYNC replica has not confirmed committing last transaction.")
                        (assoc op :type :ok :value (str e)); Exception due to down sync replica is accepted/expected
                        (assoc op :type :fail :value (str e))))))
                (assoc op :type :fail :info "Not main node"))))
  (teardown! [this _test]
    (when (= (:replication-role this) :main)
      (utils/with-session (:conn this) session
        (try
          ; Can fail for various reasons, not important at this point.
          (mgclient/detach-delete-all session)
          (catch Exception _)))))
  (close! [this _test]
    (dbclient/disconnect (:conn this))))

(defn add-nodes
  "Add nodes"
  [_ _]
  {:type :invoke :f :add :value nil})

(defn read-nodes
  "Read node count"
  [_ _]
  {:type :invoke :f :read :value nil})

(defn large-checker
  "Check if every read has a count divisible with node-num."
  []
  (reify checker/Checker
    (check [_ _ history _]
      ; For OK reads get all with :type :ok and :f :read.
      (let [ok-reads (->> history
                          (filter #(= :ok (:type %)))
                          (filter #(= :read (:f %))))
            ; Read is considered bad if count is not divisible with node-num.
            bad-reads (->> ok-reads
                           (map (fn [op]
                                  (let [count (-> op :value :count)]
                                    (when (not= 0 (mod count node-num))
                                      {:type :invalid-count
                                       :op op}))))
                           ; Filter nil values.
                           (filter identity)
                           (into []))
            ; First get all-nodes by mapping :value :node from ok-reads and storing it into a set.
            empty-nodes (let [all-nodes (->> ok-reads
                                             (map #(-> % :value :node))
                                             (reduce conj #{}))]
                          ; This code filters all-nodes to include only those nodes for which all associated ok-reads have a :count of 0.
                          (->> all-nodes
                               (filter (fn [node]
                                         (every?
                                          #(= 0 %)
                                          ; Filter all ok-reads by node, get its count and check it if it is 0.
                                          (->> ok-reads
                                               (map :value)
                                               (filter #(= node (:node %)))
                                               (map :count)))))
                               ; Filter nil values and save it into a vector.
                               (filter identity)
                               (into [])))]
        {:valid? (and
                  (empty? bad-reads)
                  (empty? empty-nodes))
         :empty-nodes empty-nodes
         :bad-reads bad-reads}))))

(defn workload
  [opts]
  {:client (Client. (:nodes-config opts))
   :checker (checker/compose
             {:large    (large-checker)
              :timeline (timeline/html)})
   :generator (mgclient/replication-gen
               (gen/mix [read-nodes add-nodes]))
   :final-generator {:clients (gen/once read-nodes) :recovery-time 40}})
