(ns memgraph.high-availability.create.nemesis
  "Memgraph nemesis for HA cluster"
  (:require [jepsen
             [nemesis :as nemesis]
             [generator :as gen]
             [util :as util]
             [control :as c]]
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
        main-instance
        (if (= 1 (count main-instances))
          (first main-instances)
          (throw (Exception. "Expected exactly one main instance.")))
        main-instance-name (:name main-instance)]
    main-instance-name))

(defn leader?
  [instance]
  (= (:role instance) "leader"))

(defn extract-coord-name
  "We could use any server. We cannot just use name because in Memgraph coordinator's name is written as 'coordinator_{id}'."
  [bolt-server]
  (first (str/split bolt-server #":")))

(defn get-current-leader
  "Returns the name of the current leader. If there is no leader or more than one leader, throws exception."
  [instances]
  (let [leaders (filter leader? instances)
        leader
        (if (= 1 (count leaders))
          (first leaders)
          (throw (Exception. "Expected exactly one leader.")))
        leader-name (extract-coord-name (:bolt_server leader)) ]
    leader-name))

(defn choose-node-to-kill
  "Chooses between the current main and the current leader. We always connect to coordinator 'n4' (free choice).
  We assume that node should always be up because even if it was killed at previous step, it should have enough time to come back
  and to be able to receive requests for show instances.
  "
  [ns]
  (let [conn (utils/open-bolt "n4")]
    (utils/with-session conn session
      (let [instances (->> (mgquery/get-all-instances session) (reduce conj []))
            main (get-current-main instances)
            leader (get-current-leader instances)
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

        node-to-kill))))

(defn node-killer
  "Responds to :start by killing a random node"
  [nodes-config]
  (node-start-stopper choose-node-to-kill
                      s/stop-node!
                      s/start-memgraph-node!
                      nodes-config))

(defn full-nemesis
  "Can kill, restart all processess and initiate network partitions."
  [nodes-config]
  (nemesis/compose
   {{:kill-node    :start
     :heal-node :stop} (node-killer nodes-config)}))

(defn nemesis-generator
  "Construct nemesis generator."
  []
  (gen/phases
   (gen/sleep 5)
   (cycle [{:type :info, :f :kill-node}
           (gen/sleep 5)
           {:type :info, :f :heal-node}
           (gen/sleep 5)])))

(defn create
  "Create a map which contains a nemesis configuration for running HA bank test."
  [nodes-config]
  {:nemesis (full-nemesis nodes-config)
   :generator (nemesis-generator)
   :final-generator (map utils/op [:heal-node])})
