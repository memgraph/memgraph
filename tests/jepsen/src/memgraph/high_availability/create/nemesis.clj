(ns memgraph.high-availability.create.nemesis
  "Memgraph nemesis for HA cluster"
  (:require [jepsen
             [nemesis :as nemesis]
             [generator :as gen]
             [net :as net]
             [util :as util]
             [control :as c]]
            [jepsen.nemesis.combined :as nemesis-combined]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [memgraph.high-availability.utils :as hautils]
            [memgraph
             [support :as s]
             [query :as mgquery]
             [utils :as utils]]))

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

(defn main-instance?
  [instance]
  (= (:role instance) "main"))

(defn get-current-main
  "Returns the name of the main instance. If there is no main instance or more than one main, throws exception."
  [instances]
  (let [main-instances (filter main-instance? instances)
        num-mains (count main-instances)
        main-instance
        (cond (= num-mains 1) (first main-instances)
              (= num-mains 0) nil
              :else (throw (Exception. "Expected at most one main instance.")))
        main-instance-name (if (nil? main-instance) nil (:name main-instance))]
    main-instance-name))


(defn choose-node-to-kill-on-coord
  "Chooses between the current main and the current leader. If there are no clear MAIN and LEADER instance in the cluster, we choose random node.
  We connect to a random coordinator. "
  [ns coord]
  (let [conn (utils/open-bolt coord)]
    (try
      (utils/with-session conn session
        (let [instances (->> (mgquery/get-all-instances session) (reduce conj []))
              main (get-current-main instances)
              leader (hautils/get-current-leader instances)
              node-to-kill (cond
                             (and (nil? main) (nil? leader)) (rand-nth ns)
                             (nil? main) leader
                             (nil? leader) main
                             :else (rand-nth [main leader]))
              node-desc (cond
                          (hautils/data-instance? node-to-kill) "current main"
                          (hautils/coord-instance? node-to-kill) "current leader"
                          :else "random node")]

          (info "Killing" node-desc ":" node-to-kill)

          node-to-kill))
      (catch org.neo4j.driver.exceptions.ServiceUnavailableException _e
        nil))))

(defn choose-node-to-kill
  [ns]
  (let [coordinators ["n4" "n5" "n6"]
        chooser (partial choose-node-to-kill-on-coord ns)]
    (or
     (some chooser coordinators)
     (rand-nth ns)))) ; fallback to default node if all goes wrong

(defn node-killer
  "Responds to :start by killing a random node."
  [nodes-config]
  (node-start-stopper choose-node-to-kill
                      s/stop-node!
                      s/start-memgraph-node!
                      nodes-config))

(defn network-disruptor
  "Responds to :start by disrupting network on chosen nodes and to :stop by restoring network configuration."
  [db]
  (nemesis-combined/packet-nemesis db))

(defn full-nemesis
  "Can kill, restart all processess and initiate network partitions."
  [db nodes-config]
  (nemesis/compose
   {{:kill-node    :start
     :heal-node :stop} (node-killer nodes-config)

    {:start-partition-halves :start
     :stop-partition-halves :stop} (nemesis/partition-random-halves)

    {:start-partition-ring :start
     :stop-partition-ring :stop} (nemesis/partition-majorities-ring)

    {:start-partition-node :start
     :stop-partition-node :stop} (nemesis/partition-random-node)

    {:start-network-disruption :start-packet
     :stop-network-disruption :stop-packet} (network-disruptor db)}))

(defn nemesis-events
  "Create a random sequence of nemesis events. Disruptions last [10-60] seconds, and the system remains undisrupted for some time afterwards."
  [nodes-config]
  (let [events [[{:type :info :f :start-partition-halves}
                 (gen/sleep (+ 10 (rand-int 51))) ; [10, 60]
                 {:type :info :f :stop-partition-halves}
                 (gen/sleep 5)]

                [{:type :info :f :start-partition-ring}
                 (gen/sleep (+ 10 (rand-int 51))) ; [10, 60]
                 {:type :info :f :stop-partition-ring}
                 (gen/sleep 5)]

                [{:type :info :f :start-partition-node}
                 (gen/sleep (+ 10 (rand-int 51))) ; [10, 60]
                 {:type :info :f :stop-partition-node}
                 (gen/sleep 5)]

                [{:type :info :f :kill-node}
                 (gen/sleep (+ 10 (rand-int 51))) ; [10, 60]
                 {:type :info :f :heal-node}
                 (gen/sleep 5)]

                [{:type :info :f :start-network-disruption :value [(keys nodes-config) net/all-packet-behaviors]}
                 (gen/sleep (+ 10 (rand-int 51))) ; [10, 60]
                 {:type :info :f :stop-network-disruption}
                 (gen/sleep 5)]]]

    (mapcat identity (repeatedly #(rand-nth events)))))

(defn create
  "Create a map which contains a nemesis configuration for running HA create test."
  [db nodes-config nemesis-start-sleep]
  {:nemesis (full-nemesis db nodes-config)
   :generator (gen/phases
               (gen/sleep nemesis-start-sleep) ; Enough time for cluster setup to finish
               (nemesis-events nodes-config))
   :final-generator (map utils/op [:stop-partition-ring :stop-partition-halves :stop-partition-node :heal-node :stop-network-disruption])})
