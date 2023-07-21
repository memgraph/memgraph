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



  ; (gen/phases (->> (gen/mix [(repeat {:f :read})
  ;                              (map (fn [x] {:f :write, :value x}) (range))])
  ;                    (gen/stagger 1/10)
  ;                    (gen/nemesis (->> (cycle [{:f :break}
  ;                                              {:f :repair}])
  ;                                      (gen/stagger 5)))
  ;                    (gen/time-limit 30))
  ;               (gen/log \"Recovering\")
  ;               (gen/nemesis {:f :repair})
  ;               (gen/sleep 10)
  ;               (gen/log \"Final read\")
  ;               (gen/clients (gen/each-thread (gen/until-ok {:f :read}))))


; (defn full-generator
;   "Construct nemesis generator."
;   [opts]
;   (->> [(when (:kill-node? opts)
;           [(cycle (map op [:kill-node :restart-node]))])
;         (when (:partition-halves? opts)
;           [(cycle (map op [:start-partition-halves :stop-partition-halves]))])]
;        (apply concat)
;        (gen/phases (gen/sleep 40))
;        gen/mix
;        (gen/stagger (:interval opts))
;        ))


(defn events-sequence
  "Constructs sequence of Nemesis events."
  [opts]
  (apply concat
      (when (:kill-node? opts)
          [(cycle (map op [:kill-node :restart-node]))])
      (when (:partition-halves? opts)
          [(cycle (map op [:start-partition-halves :stop-partition-halves]))])
  ))

; (defn full-generator
;   "Construct nemesis generator."
;   [opts]
;   (->> (events-sequence opts)
;        (gen/phases (gen/sleep 40))
;        gen/mix
;        (gen/stagger (:interval opts))
;        ))


(defn full-generator
  "Construct nemesis generator."
  [opts]
  (->>  ; (gen/sleep 5)
        (gen/mix (apply concat
        [[(cycle (map op [:kill-node :restart-node]))]
        [(cycle (map op [:start-partition-halves :stop-partition-halves]))]]
        ))
        (gen/stagger (:interval opts))))


; (defn full-generator
;   "Test."
;   [opts]
;    (->> (gen/mix [(repeat {:f :read})
;                    (map (fn [x] {:f :write, :value x}) (range))])
;          (gen/stagger 1/10)
;          (gen/time-limit 30)))

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
