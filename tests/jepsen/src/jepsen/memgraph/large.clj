(ns jepsen.memgraph.large
  "Large write test"
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.memgraph.client :as c]))

(def node-num 100000)

(dbclient/defquery get-node-count
  "MATCH (n:Node) RETURN count(n) as c;")

(defn create-nodes-builder
  []
  (dbclient/create-query
    (str "UNWIND range(1, " node-num ") AS i "
         "CREATE (n:Node:Additional {id: i, property1: 0, property2: 1, property3: 2});")))

(def create-nodes (create-nodes-builder))

(c/replication-client Client []
  (open! [this test node]
    (c/replication-open-connection this node node-config))
  (setup! [this test]
    (when (= replication-role :main)
      (c/with-session conn session
        (c/detach-delete-all session)
        (create-nodes session))))
  (invoke! [this test op]
    (c/replication-invoke-case (:f op)
      :read   (c/with-session conn session
                (assoc op
                       :type :ok
                       :value {:count (->> (get-node-count session)
                                           first
                                           :c)
                               :node node}))
      :add    (if (= replication-role :main)
                (c/with-session conn session
                  (create-nodes session)
                  (assoc op :type :ok))
                (assoc op :type :fail))))
  (teardown! [this test]
    (when (= replication-role :main)
      (c/with-session conn session
        (try
          (c/detach-delete-all session)
          (catch Exception e
            ; Deletion can give exception if a sync replica is down, that's expected
            (assoc :type :fail :info (str e)))))
  (close! [_ est]
    (dbclient/disconnect conn)))

(defn add-nodes
  "Add nodes"
  [test process]
  {:type :invoke :f :add :value nil})

(defn read-nodes
  "Read node count"
  [test process]
  {:type :invoke :f :read :value nil})

(defn large-checker
  "Check if every read has a count divisible with node-num."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [ok-reads (->> history
                          (filter #(= :ok (:type %)))
                          (filter #(= :read (:f %))))
            bad-reads (->> ok-reads
                           (map (fn [op]
                                  (let [count (-> op :value :count)]
                                    (when (not= 0 (mod count node-num))
                                      {:type :invalid-count
                                       :op op}))))
                           (filter identity)
                           (into []))
            empty-nodes (let [all-nodes (->> ok-reads
                                             (map #(-> % :value :node))
                                             (reduce conj #{}))]
                          (->> all-nodes
                               (filter (fn [node]
                                        (every?
                                          #(= 0 %)
                                          (->> ok-reads
                                               (map :value)
                                               (filter #(= node (:node %)))
                                               (map :count)))))
                               (filter identity)
                               (into [])))]
        {:valid? (and
                   (empty? bad-reads)
                   (empty? empty-nodes))
         :empty-nodes empty-nodes
         :bad-reads bad-reads}))))

(defn workload
  [opts]
  {:client (Client. nil nil nil (:node-config opts))
   :checker (checker/compose
              {:large    (large-checker)
               :timeline (timeline/html)})
   :generator (c/replication-gen
                (gen/mix [read-nodes add-nodes]))
   :final-generator {:gen (gen/once read-nodes) :recovery-time 40}})
