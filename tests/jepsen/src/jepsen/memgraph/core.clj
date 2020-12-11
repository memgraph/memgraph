(ns jepsen.memgraph.core
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
                    [core :as jepsen]
                    [checker :as checker]
                    [nemesis :as nemesis]
                    [generator :as gen]
                    [tests :as tests]]
            [slingshot.slingshot :refer [try+ throw+]]
            [jepsen.memgraph [basic :as basic]
                             [bank :as bank]
                             [support :as s]
                             [edn :as e]]))

(def workloads
  "A map of workload names to functions that can take opts and construct
   workloads."
  {:basic basic/workload
   :bank  bank/workload})

(defn memgraph-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)
        gen      (->> (:generator workload)
                      (gen/nemesis
                         (cycle [(gen/sleep 5)
                                 {:type :info, :f :start}
                                 (gen/sleep 5)
                                 {:type :info, :f :stop}]))
                        (gen/time-limit (:time-limit opts)))
        gen      (if-let [final-generator (:final-generator workload)]
                   (gen/phases gen
                               (gen/log "Healing cluster.")
                               (gen/nemesis (gen/once {:type :info, :f :stop}))
                               (gen/log "Waiting for recovery")
                               (gen/sleep 10)
                               (gen/clients final-generator))
                   gen)]
    (merge tests/noop-test
           opts
           {:pure-generators true
            :name            (str "test-" (name (:workload opts)))
            :db              (s/db (:package-url opts) (:local-binary opts))
            :client          (:client workload)
            :checker         (checker/compose
                               {;; :stats      (checker/stats) CAS always fails
                                ;;                             so enable this
                                ;;                             if all test have
                                ;;                             at least 1 ok op
                                :exceptions (checker/unhandled-exceptions)
                                :perf       (checker/perf)
                                :workload   (:checker workload)})
            :nemesis         (nemesis/partition-random-halves)
            :generator       gen})))

(defn default-node-configuration
  "Creates default replication configuration for nodes.
  All of them are replicas in sync mode."
  [nodes]
  (reduce (fn [cur n]
            (conj cur {n
                       {:replication-role :replica
                        :replication-mode :sync}}))
          {}
          nodes))

(defn merge-node-configurations
  "Merge user defined configuration with default configuration.
  Check if the configuration is valid."
  [nodes node-configs]
  (when-not (every? (fn [config]
                      (= 1
                         (count
                             (filter #(= (:replication-role %) :main) (vals config)))))
                    node-configs)
    (throw (Exception. "Invalid node configuration. There can only be one :main.")))
  (map #(merge (default-node-configuration nodes) %) node-configs))

(def cli-opts
  "CLI options for tests."
  [[nil "--package-url URL" "What package of Memgraph should we test?"
    :default nil]
   [nil "--local-binary PATH" "Ignore package; use this local binary instead."
    :default "/opt/memgraph/memgraph"]
   ["-w" "--workload NAME" "Test workload to run"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]
   [nil "--node-configs PATH" "Path to the node configuration file."
    :parse-fn #(-> % e/load-configuration)]])

(defn all-tests
  "Takes base CLI options and constructs a sequence of test options."
  [opts]
  (let [counts    (range (:test-count opts))
        workloads (if-let [w (:workload opts)] [w] (keys workloads))
        node-configs (if (:node-configs opts)
                       (merge-node-configurations (:nodes opts) (:node-configs opts))
                       [(default-node-configuration (:nodes opts))])
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
                   (cli/single-test-cmd {:test-fn memgraph-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
