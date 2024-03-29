(ns jepsen.memgraph.core
  (:gen-class)
  (:import (java.net URI))
  (:require
   [neo4j-clj.core :as dbclient]
   [clojure.tools.logging :refer :all]
   [clojure.java.shell :refer [sh]]
   [jepsen [cli :as cli]
    [checker :as checker]
    [client :as client]
    [generator :as gen]
    [tests :as tests]]
   [jepsen.memgraph
    [utils :as utils]
    [bank :as bank]
    [large :as large]
    [ha :as ha]
    [support :as support]
    [nemesis :as nemesis]
    [edn :as e]]))

(def workloads
  "A map of workload names to functions that can take opts and construct
   workloads."
  {:bank                      bank/workload
   :large                     large/workload
   :high_availability         ha/test-setup})

(def nemesis-configuration
  "Nemesis configuration"
  {:interval          5
   :kill-node?        true
   :partition-halves? true})

(defn memgraph-ha-test
  "Given an options map from the command line runner constructs a test map for HA tests."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)]
    (println "memgraph-ha-test" opts)
    (merge tests/noop-test
           opts
           {:pure-generators true
            :nodes           (keys (:node-config opts))
            :name            (str "test-" (name (:workload opts)))
            :db              (support/db opts)
            :client          (:client workload)})))

(defn memgraph-test
  "Given an options map from the command line runner constructs a test map."
  [opts]
  (println "memgraph-test")
  (let [workload ((get workloads (:workload opts)) opts)  ;opts are options provided to a function from workloads
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
            :nodes           (keys (:node-config opts))
            :name            (str "test-" (name (:workload opts)))
            :db              (support/db opts)
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

(defn validate-node-configurations
  "Validate that configuration of node configs is valid."
  [node-configs]

  (when-not (every? (fn [config]
                      (= 1
                         (count
                          (filter
                           #(= (:replication-role %) :main)
                           (vals config)))))
                    node-configs)
    (throw (Exception. "Invalid node configuration. There can only be one :main.")))

  (doseq [node-config node-configs]
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
  node-configs)

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
  "Takes base CLI options and constructs a single test. If node-configs is a list, only the 1st config is used."
  [opts]
  (println "single-test" opts)
  (let [workload (if (:workload opts)
                   (:workload opts)
                   (throw (Exception. "Workload undefined!")))
        node-config (if (:node-configs opts)
                      (first (if (= workload :high_availability)
                               (:node-configs opts) ; If high availability, no need to resolve hostnames and validate yet
                               (map resolve-all-node-hostnames (validate-node-configurations (:node-configs opts)))))
                      (throw (Exception. "Node configs undefined!")))]
    (if (= workload :high_availability)
      ;; Use node-config directly for high availability workload
      (memgraph-ha-test (assoc opts :node-config node-config :workload workload))
      ;; Use node-config for other workloads
      (let [test-opts (assoc opts :node-config node-config :workload workload)]
        (memgraph-test test-opts)))))

(defn all-tests
  "Takes base CLI options and constructs a sequence of test options."
  [opts]
  (let [workloads (if-let [w (:workload opts)] [w] (keys workloads))
        node-configs (if (:node-configs opts)
                       (map resolve-all-node-hostnames (validate-node-configurations (:node-configs opts)))
                       (throw (Exception. "Node config is missing")))
        test-opts (for [node-config node-configs workload workloads]
                    (assoc opts
                           :node-config node-config
                           :workload workload))]
    (println "test-opts" test-opts)
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
