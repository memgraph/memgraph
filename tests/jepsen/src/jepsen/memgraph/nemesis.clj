(ns jepsen.memgraph.nemesis
  "Memgraph nemesis"
  (:require [jepsen [nemesis :as nemesis]
                    [generator :as gen]]
            [jepsen.memgraph.support :as s]))

(defn node-killer
  "Responds to :start by killing a random node, and to :stop
  by resuming them."
  []
  (nemesis/node-start-stopper identity
                              s/stop-node!
                              s/start-node!))

(defn full-nemesis
  "Can kill and restart all processes and initiate network partitions."
  [opts]
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
  [opts]
  (->> [(when (:kill-node? opts)
          [(cycle (map op [:kill-node :restart-node]))])
        (when (:partition-halves? opts)
          [(cycle (map op [:start-partition-halves :stop-partition-halves]))])]
       (apply concat)
       gen/mix
       (gen/stagger (:interval opts))
       (gen/phases (gen/sleep 10))))

(defn nemesis
  "Composite nemesis and generator"
  [opts]
  {:nemesis (full-nemesis opts)
   :generator (full-generator opts)
   :final-generator
   (->> [(when (:partition-halves? opts) :stop-partition-halves)
         (when (:kill-node? opts) :restart-node)]
        (remove nil?)
        (map op))})
