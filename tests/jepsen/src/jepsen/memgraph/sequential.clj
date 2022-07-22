(ns jepsen.memgraph.sequential
  "Sequential test"
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.memgraph.client :as c]))

(dbclient/defquery get-all-nodes
  "MATCH (n:Node) RETURN n ORDER BY n.id;")

(dbclient/defquery create-node
  "CREATE (n:Node {id: $id});")

(dbclient/defquery delete-node-with-id
  "MATCH (n:Node {id: $id}) DELETE n;")

(def next-node-for-add (atom 0))

(defn add-next-node
  "Add a new node with its id set to the next highest"
  [conn]
  (when (dbclient/with-transaction conn tx
      (create-node tx {:id (swap! next-node-for-add identity)}))
      (swap! next-node-for-add inc)))

(def next-node-for-delete (atom 0))

(defn delete-oldest-node
  "Delete a node with the lowest id"
  [conn]
  (when (dbclient/with-transaction conn tx
      (delete-node-with-id tx {:id (swap! next-node-for-delete identity)}))
      (swap! next-node-for-delete inc)))

(c/replication-client Client []
  (open! [this test node]
    (c/replication-open-connection this node node-config))
  (setup! [this test]
    (when (= replication-role :main)
      (c/with-session conn session
        (c/detach-delete-all session)
        (create-node session {:id 0}))))
  (invoke! [this test op]
    (c/replication-invoke-case (:f op)
      :read   (c/with-session conn session
                (assoc op
                       :type :ok
                       :value {:ids (->> (get-all-nodes session)
                                         (map #(-> % :n :id))
                                         (reduce conj []))
                               :node node}))
      :add    (if (= replication-role :main)
                (try
                  (assoc op :type (if (add-next-node conn) :ok :fail))
                  (catch Exception e
                    ; Transaction can fail on serialization errors
                    (assoc op :type :fail :info (str e))))
                (assoc op :type :fail))
      :delete (if (= replication-role :main)
                (try
                  (assoc op :type (if (delete-oldest-node conn) :ok :fail))
                  (catch Exception e
                    ; Transaction can fail on serialization errors
                    (assoc op :type :fail :info (str e))))
                (assoc op :type :fail))))
  (teardown! [this test]
    (when (= replication-role :main)
      (c/with-session conn session
        (try
          (c/detach-delete-all session)
          (catch Exception e
            ; Deletion can give exception if a sync replica is down, that's expected
            (assoc :type :fail :info (str e))))))
  (close! [_ est]
    (dbclient/disconnect conn)))

(defn add-node
  "Add node with id set to current_max_id + 1"
  [test process]
  {:type :invoke :f :add :value nil})

(defn read-ids
  "Read all current ids of nodes"
  [test process]
  {:type :invoke :f :read :value nil})

(defn delete-node
  "Delete node with the lowest id"
  [test process]
  {:type :invoke :f :delete :value nil})

(defn strictly-increasing
  [coll]
  (every?
    #(< (first %) (second %))
    (partition 2 1 coll)))

(defn increased-by-1
  [coll]
  (every?
    #(= (inc (first %)) (second %))
    (partition 2 1 coll)))

(defn sequential-checker
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
                                  (let [ids (-> op :value :ids)]
                                    (when (not-empty ids)
                                      (cond ((complement strictly-increasing) ids)
                                            {:type :not-increasing-ids
                                             :op op})))))

                                            ;; if there are multiple threads not sure how to guarante that the ids are created in order
                                            ;;((complement increased-by-1) ids)
                                            ;;{:type :ids-missing
                                            ;; :op op})))))
                           (filter identity)
                           (into []))
            empty-nodes (let [all-nodes (->> ok-reads
                                             (map #(-> % :value :node))
                                             (reduce conj #{}))]
                          (->> all-nodes
                               (filter (fn [node]
                                        (every?
                                          empty?
                                          (->> ok-reads
                                               (map :value)
                                               (filter #(= node (:node %)))
                                               (map :ids)))))
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
              {:sequential (sequential-checker)
               :timeline   (timeline/html)})
   :generator (c/replication-gen
                (gen/phases (cycle [(gen/time-limit 1 (gen/mix [read-ids add-node]))
                                    (gen/once delete-node)])))
   :final-generator (gen/once read-ids)})
