(ns jepsen.memgraph.hanemesis
  "Memgraph nemesis for HA cluster"
  (:require [jepsen [nemesis :as nemesis]
             [generator :as gen]]
            [jepsen.memgraph.utils :as utils]))

(defn full-nemesis
  "Can kill and restart all processess and initiate network partitions."
  []
  (nemesis/compose
   {{:start-partition-halves :start
     :stop-partition-halves  :stop} (nemesis/partition-random-halves)}))

(defn full-generator
  "Construct nemesis generator."
  []
  (gen/phases (cycle [(gen/sleep 5)
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
        (map utils/op))})
