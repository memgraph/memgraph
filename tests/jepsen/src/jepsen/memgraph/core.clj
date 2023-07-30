(ns jepsen.memgraph.core
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.shell :refer [sh]]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [control :as c]
                    [core :as jepsen]
                    [generator :as gen]
                    [tests :as tests]]
            [slingshot.slingshot :refer [try+ throw+]]
            [jepsen.memgraph [basic :as basic]
                             [bank :as bank]
                             [large :as large]
                             [support :as s]
                             [nemesis :as nemesis]
                             [edn :as e]]))

(def workloads
  "A map of workload names to functions that can take opts and construct
   workloads."
   {:bank       bank/workload
    :large      large/workload})

(def nemesis-configuration
  "Nemesis configuration"
  {:interval          5
   :kill-node?        true
   :partition-halves? true})

(defn memgraph-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)
        nemesis  (nemesis/nemesis nemesis-configuration)
        gen      (->> (:generator workload)
                      (gen/nemesis (:generator nemesis))
                      (gen/time-limit (:time-limit opts)))
        gen      (if-let [final-generator (:final-generator workload)]
                   (gen/phases gen
                               (gen/log "Healing cluster.")
                               (gen/nemesis (:final-generator nemesis))
                               (gen/log "Waiting for recovery")
                               (gen/sleep (:recovery-time final-generator))
                               (gen/clients (:gen final-generator)))
                   gen)]
    (merge tests/noop-test
           opts
           {:pure-generators true
            :name            (str "test-" (name (:workload opts)))
            :db              (s/db opts)
            :client          (:client workload)
            :checker         (checker/compose
                               {:stats      (checker/stats)
                                :exceptions (checker/unhandled-exceptions)
                                ;:perf       (checker/perf) really exepnsive
                                :workload   (:checker workload)})
            :nemesis         (:nemesis nemesis)
            :generator       gen})))

(defn resolve-hostname
  "Resolve hostnames to ip address"
  [host]
  (first
    (re-find
      #"(\d{1,3}(.\d{1,3}){3})"
      (:out (sh "getent" "hosts" host)))))

(defn resolve-all-node-hostnames
  "Resolve all hostnames in config and assign it to the node"
  [node-config]
  (reduce (fn [curr node]
            (let [k (first node)
                  v (second node)]
              (assoc curr
                     k (assoc v
                              :ip (resolve-hostname k)))))
          {}
          node-config))

(defn throw-if-key-missing-in-any
  [map-coll key error-msg]
  (when-not (every? #(contains? % key) map-coll)
    (throw (Exception. error-msg))))

(defn merge-node-configurations
  "Merge user defined configuration with default configuration.
  Check if the configuration is valid."
  [nodes node-configs]


  (when-not (every? (fn [config]
                      (= 1
                         (count
                             (filter
                               #(= (:replication-role %) :main)
                               (vals config)))))
                    node-configs)
    (throw (Exception. "Invalid node configuration. There can only be one :main.")))


  (doseq [node-config node-configs]
    (when-not (= (count node-config) (count nodes))
      (throw (Exception. "Invalid node configuration. Configuration not specified for all nodes. Check your .edn file.")))

    (when-not (every? #(contains? node-config %) nodes)
      (throw (Exception. "Invalid node configuration. Configuration missing for some of nodes [:n1 :n2 :n3 :n4 :n5]. Check your .edn file.")))

    (let [replica-nodes-configs (filter
                                  #(= (:replication-role %) :replica)
                                  (vals node-config))]
      (throw-if-key-missing-in-any
        replica-nodes-configs
        :port
        (str "Invalid node configuration. "
             "Every replication node requires "
             ":port to be defined."))
      (throw-if-key-missing-in-any
        replica-nodes-configs
        :replication-mode
        (str "Invalid node configuration. "
             "Every replication node requires "
             ":replication-mode to be defined."))))

  (map (fn [node-config] (resolve-all-node-hostnames node-config))
       node-configs))

(def cli-opts
  "CLI options for tests."
  [[nil "--package-url URL" "What package of Memgraph should we test?"
    :default nil
    :validate [nil? "Memgraph package-url setup not yet implemented."]]
   [nil "--local-binary PATH" "Ignore package; use this local binary instead."
    :default "/opt/memgraph/memgraph"
    :validate [#(and (some? %) (not-empty %)) "local-binary should be defined."]]
   ["-w" "--workload NAME" "Test workload to run"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]
   [nil "--node-configs PATH" "Path to a file containing a list of node config."
    :parse-fn #(-> % e/load-configuration)]])

(defn single-test
  "Takes base CLI options and constructs a single test."
  [opts]
  (let [workload (if (:workload opts)
                   (:workload opts)
                   (throw (Exception. "Workload undefined")))
        node-config (if (:node-configs opts)
                      (first (merge-node-configurations (:nodes opts) (list (first (:node-configs opts)))))
                      (throw (Exception. "Node configs undefined")))
        test-opts (assoc opts
                         :node-config node-config
                         :workload workload)]
    (memgraph-test test-opts)))

(defn all-tests
  "Takes base CLI options and constructs a sequence of test options."
  [opts]
  (let [counts    (range (:test-count opts))
        workloads (if-let [w (:workload opts)] [w] (keys workloads))
        node-configs (if (:node-configs opts)
                       (merge-node-configurations (:nodes opts) (:node-configs opts))
                       (throw (Exception. "Node config is missing")))
        test-opts (for [i counts c node-configs w workloads]
                   (assoc opts
                          :node-config c
                          :workload w))]
    (map memgraph-test test-opts)))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/test-all-cmd {:tests-fn all-tests
                                      :opt-spec cli-opts})
                   (cli/single-test-cmd {:test-fn single-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
