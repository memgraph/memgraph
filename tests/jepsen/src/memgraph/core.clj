(ns memgraph.core
  (:gen-class)
  (:require
   [jepsen [cli :as cli]
    [checker :as checker]
    [generator :as gen]
    [tests :as tests]]
   [clojure.tools.logging :refer [info]]
   [memgraph
    [bank :as bank]
    [large :as large]
    [habank :as habank]
    [support :as support]
    [hanemesis :as hanemesis]
    [nemesis :as nemesis]
    [utils :as utils]
    [edn :as e]]))

(def workloads
  "A map of workload names to functions that can take opts and construct
   workloads."
  {:bank                      bank/workload
   :large                     large/workload
   :habank                    habank/workload})

(def nemesis-configuration
  "Nemesis configuration"
  {:interval          5
   :kill-node?        true
   :partition-halves? true})

(defn compose-gen
  "Composes final generator used in the test from workload and nemesis configuration. Used for HA testing and single-instance testing.
  "
  [opts workload nemesis]
  (gen/time-limit
   (:time-limit opts)
   (gen/nemesis (:generator nemesis) (:generator workload))))

(defn memgraph-ha-test
  "Given an options map from the command line runner constructs a test map for HA tests."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)
        nemesis (hanemesis/nemesis nemesis-configuration (:nodes-config opts))
        gen (compose-gen opts workload nemesis)]

    (merge tests/noop-test
           opts
           {:pure-generators true
            :name            (str "test-" (name (:workload opts)))
            :db              (support/db opts)
            :client          (:client workload)
            :checker         (checker/compose
                              {:stats      (checker/stats)
                               :exceptions (utils/unhandled-exceptions)
                               :log-checker (checker/log-file-pattern #"assert|NullPointerException|json.exception.parse_error" "memgraph.log")
                               :workload   (:checker workload)})
            :nodes           (keys (:nodes-config opts))
            :nemesis        (:nemesis nemesis)
            :generator      gen})))

(defn memgraph-test
  "Given an options map from the command line runner constructs a test map."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)
        nemesis  (nemesis/nemesis nemesis-configuration)
        gen (compose-gen opts workload nemesis)
        ; If final generator exists in the workload, then modify gen, otherwise use what you already have.
        gen      (if-let [final-generator (:final-generator workload)]
                   (gen/phases gen
                               (gen/log "Healing cluster.")
                               (gen/nemesis (:final-generator nemesis))
                               (gen/log "Waiting for recovery")
                               (gen/sleep (:recovery-time final-generator))
                               (gen/clients (:clients final-generator)))
                   gen)]
    (merge tests/noop-test
           opts
           {:pure-generators true
            :name            (str "test-" (name (:workload opts)))
            :db              (support/db opts)
            :client          (:client workload)
            :checker         (checker/compose
                              {:stats      (checker/stats)
                               :exceptions (utils/unhandled-exceptions)
                               :log-checker (checker/log-file-pattern #"assert|NullPointerException|json.exception.parse_error" "memgraph.log")
                               :workload   (:checker workload)})
            :nemesis         (:nemesis nemesis)
            :generator       gen})))

(defn throw-if-key-missing-in-any
  [map-coll key error-msg]
  (when-not (every? #(contains? % key) map-coll)
    (throw (Exception. error-msg))))

(defn validate-nodes-configuration
  "Validate that configuration of nodes is valid."
  [nodes-config]

  (when-not (= 1
               (count
                (filter
                 #(= (:replication-role %) :main)
                 (vals nodes-config))))
    (throw (Exception. "Invalid node configuration. There can only be one :main.")))

  (let [replicas-config (filter
                         #(= (:replication-role %) :replica)
                         (vals nodes-config))]
    (throw-if-key-missing-in-any
     replicas-config
     :port
     (str "Invalid node configuration. "
          "Every replica requires "
          ":port to be defined."))
    (throw-if-key-missing-in-any
     replicas-config
     :replication-mode
     (str "Invalid node configuration. "
          "Every replica requires "
          ":replication-mode to be defined.")))
  nodes-config)

(defn single-test
  "Takes base CLI options and constructs a single test."
  [opts]

  (info "Results")

  (let [workload (if (:workload opts)
                   (:workload opts)
                   (throw (Exception. "Workload undefined!")))
        nodes-config (if (:nodes-config opts)
                       (if (or (= workload :high_availability) (= workload :habank))
                         (:nodes-config opts)
                         (validate-nodes-configuration (:nodes-config opts))) ; validate only if not HA
                       (throw (Exception. "Nodes config flag undefined!")))
        ; Bank test relies on 100% durable Memgraph, fsyncing after every txn.
        sync-after-n-txn (if (or (= workload :bank) (= workload :habank))
                           1
                           100000)
        licence (when (:license opts)
                  (:license opts))
        organization (when (:organization opts)
                       (:organization opts))
        test-opts (merge opts
                         {:workload workload
                          :nodes-config nodes-config
                          :sync-after-n-txn sync-after-n-txn
                          :license licence
                          :organization organization})]
    (if (or (= workload :high_availability) (= workload :habank))
      (memgraph-ha-test test-opts)
      (memgraph-test test-opts))))

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
   ["-l" "--license KEY" "Memgraph license key"
    :default nil]
   ["-o" "--organization ORGANIZATION" "Memgraph organization name" :default nil]
   [nil "--nodes-config PATH" "Path to a file containing the config for each node."
    :parse-fn #(-> % e/load-configuration)]])

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn single-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))