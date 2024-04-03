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


(defn nemesis-events
  "Constructs events for nemesis based on provided options."
  [opts]
  (apply concat
      [(when (:kill-node? opts)
          [(cycle (map op [:kill-node :restart-node]))])
      (when (:partition-halves? opts)
          [(cycle (map op [:start-partition-halves :stop-partition-halves]))])]
  ))


(defn full-generator
  "Construct nemesis generator."
  [opts]
  (gen/phases
   (gen/log "Waiting replicas to get data from main.")
   (gen/sleep 40)
   (->>
    (gen/mix (nemesis-events opts))
    (gen/stagger (:interval opts)))))


(defn nemesis
  "Composite nemesis and generator"
  [opts]
  {:nemesis (full-nemesis)
   :generator (full-generator opts)
   :final-generator
   (->> [(when (:partition-halves? opts) :stop-partition-halves)
         (when (:kill-node? opts) :restart-node)]
        (remove nil?)
        (map op))})
