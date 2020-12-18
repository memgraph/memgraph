(ns jepsen.memgraph.large
  "Large test"
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.memgraph.client :as c]))

(dbclient/defquery get-node-count
  "MATCH (n:Node) RETURN count(n) as c;")

(dbclient/defquery create-nodes
  "UNWIND range(1, 100000) as i
  CREATE (n:Node:Additional {id: i, property: 0, property2: 1, property3: 2});")

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
        (c/detach-delete-all session))))
  (close! [_ est]
    (dbclient/disconnect conn)))

(defn add-nodes
  "Add nodes"
  [test process]
  {:type :invoke :f :add :value nil})

(defn read-nodes
  "Read nodes"
  [test process]
  {:type :invoke :f :read :value nil})

(defn large-checker
  "Check if all nodes have nodes with ids that are strictly increasing by 1.
  All nodes need to have at leas 1 non-empty read."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [ok-reads (->> history
                          (filter #(= :ok (:type %)))
                          (filter #(= :read (:f %))))
            bad-reads (->> ok-reads
                           (map (fn [op]
                                  (let [count (-> op :value :count)]
                                    (when (not= 0 (mod count 100000))
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
   :final-generator (gen/once read-nodes)})
