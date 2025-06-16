(ns memgraph.replication.nemesis
  "Memgraph nemesis"
  (:require [jepsen [nemesis :as nemesis]
             [util :as util]
             [generator :as gen]]
            [memgraph
             [support :as s]
             [utils :as utils]]))

(defn node-killer
  "Responds to :start by killing a random subset of nodes, and to :stop
  by resuming them."
  []
  (nemesis/node-start-stopper util/random-nonempty-subset
                              s/stop-node!
                              s/start-node!))

(defn full-nemesis
  "Can kill and restart all processes and initiate network partitions."
  []
  (nemesis/compose
   {{:kill-node    :start
     :restart-node :stop} (node-killer)
    {:start-partition-halves :start
     :stop-partition-halves  :stop} (nemesis/partition-random-halves)}))

(defn nemesis-generator
  "Construct nemesis generator."
  []
  (gen/phases
   (gen/sleep 5)
   (cycle [(gen/sleep 5)
           {:type :info, :f :kill-node}
           (gen/sleep 5)
           {:type :info, :f :restart-node}
           (gen/sleep 5)
           {:type :info, :f :start-partition-halves}
           (gen/sleep 5)
           {:type :info, :f :stop-partition-halves}])))

(defn create
  "Create a map which contains a nemesis configuration for running a replication test."
  []
  {:nemesis (full-nemesis)
   :generator (nemesis-generator)
   :final-generator (map utils/op [:stop-partition-halves :restart-node])})
