(ns jepsen.memgraph.hanemesis
  "Memgraph nemesis for HA cluster"
  (:require [jepsen
             [nemesis :as nemesis]
             [generator :as gen]
             [util :as util]
             [control :as c]]
            [jepsen.memgraph.support :as s]
            [jepsen.memgraph.utils :as utils]))

(defn node-start-stopper
  "Takes a targeting function which, given a list of nodes, returns a single
  node or collection of nodes to affect, and two functions `(start! test node)`
  invoked on nemesis start, and `(stop! test node nodes-config)` invoked on nemesis stop.
  Also takes a nodes-config map needed to restart nodes.
  Returns a nemesis which responds to :start and :stop by running the start!
  and stop! fns on each of the given nodes. During `start!` and `stop!`, binds
  the `jepsen.control` session to the given node, so you can just call `(c/exec
  ...)`.

  The targeter can take either (targeter test nodes) or, if that fails,
  (targeter nodes).

  Re-selects a fresh node (or nodes) for each start--if targeter returns nil,
  skips the start. The return values from the start and stop fns will become
  the :values of the returned :info operations from the nemesis, e.g.:

      {:value {:n1 [:killed \"java\"]}}"
  [targeter start! stop! nodes-config]
  (let [nodes (atom nil)]
    (reify nemesis/Nemesis
      (setup! [this _] this)

      (invoke! [_ test op]
        (locking nodes
          (assoc op :type :info, :value
                 (case (:f op)
                   :start (let [ns (:nodes test)
                                ns (try (targeter test ns)
                                        (catch clojure.lang.ArityException _
                                          (targeter ns)))
                                ns (util/coll ns)]
                            (if ns
                              (if (compare-and-set! nodes nil ns)
                                (c/on-many ns (start! test c/*host*))
                                (str "nemesis already disrupting "
                                     (pr-str @nodes)))
                              :no-target))
                   :stop (if-let [ns @nodes]
                           (let [value (c/on-many ns (stop! test c/*host* nodes-config))]
                             (reset! nodes nil)
                             value)
                           :not-started)))))

      (teardown! [_ _]))))

(defn node-killer
  "Responds to :start by killing a random subset of data instances and 1 coordinator at most. Responds to :stop
  by resuming them."
  [nodes-config]
  (node-start-stopper utils/random-nonempty-subset
                      s/stop-node!
                      s/start-memgraph-node!
                      nodes-config))

(defn full-nemesis
  "Can kill and restart all processess and initiate network partitions."
  [nodes-config]
  (nemesis/compose
   {{:kill-node    :start
     :restart-node :stop} (node-killer nodes-config)
    {:start-partition-halves :start
     :stop-partition-halves  :stop} (nemesis/partition-random-halves)}))

(defn full-generator
  "Construct nemesis generator."
  []
  (gen/phases (cycle [(gen/sleep 5)
                      {:type :info, :f :kill-node}
                      (gen/sleep 5)
                      {:type :info, :f :restart-node}
                      (gen/sleep 5)
                      {:type :info, :f :start-partition-halves}
                      (gen/sleep 5)
                      {:type :info, :f :stop-partition-halves}])))

(defn nemesis
  "Composite nemesis and generator"
  [nemesis-opts nodes-config]
  {:nemesis (full-nemesis nodes-config)
   :generator (full-generator)
   :final-generator
   (->> [(when (:partition-halves? nemesis-opts) :stop-partition-halves)
         (when (:kill-node? nemesis-opts) :restart-node)]
        (remove nil?)
        (map utils/op))})
