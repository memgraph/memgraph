(ns jepsen.memgraph.nemesis
  "Memgraph nemesis"
  (:require [jepsen [nemesis :as nemesis]
             [util :as util]
             [generator :as gen]]
            [jepsen.memgraph.support :as s]))

(defn node-killer
  "Responds to :start by killing a random node, and to :stop
  by resuming them."
  []
  (nemesis/node-start-stopper util/random-nonempty-subset
                              s/stop-node!
                              s/start-node!))

(defn full-nemesis
  "Can kill and restart all processess and initiate network partitions."
  []
  (nemesis/compose
   {{:kill-node    :start
     :restart-node :stop} (node-killer)
    {:start-partition-halves :start
     :stop-partition-halves  :stop} (nemesis/partition-random-halves)}))

(defn op
  "Construct a nemesis op"
  [f]
  {:type :info :f f})

(defn full-generator
  "Construct nemesis generator."
  []
  (gen/phases (cycle [
                      (gen/sleep 5)
                      {:type :info, :f :kill-node}
                      (gen/sleep 5)
                      {:type :info, :f :restart-node}
                      (gen/sleep 5)
                      {:type :info, :f :start-partition-halves}
                      (gen/sleep 5)
                      {:type :info, :f :stop-partition-halves}])))

(defn nemesis
  "Composite nemesis and generator"
  [opts]
  {:nemesis (full-nemesis)
   :generator (full-generator)
   :final-generator
   (->> [(when (:partition-halves? opts) :stop-partition-halves)
         (when (:kill-node? opts) :restart-node)]
        (remove nil?)
        (map op))})
