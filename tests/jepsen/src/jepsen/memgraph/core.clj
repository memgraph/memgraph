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
    [support :as support]
    [nemesis :as nemesis]
    [edn :as e]]))

(defrecord HAClient [conn]
  client/Client
  (open! [this test node]
    (dbclient/connect (URI. (utils/get-instance-url node 7687)) "" "")
    this)

  (setup! [this test])

  (invoke! [_ test op])

  (teardown! [this test])

  (close! [_ test]))

(def workloads
  "A map of workload names to functions that can take opts and construct
   workloads."
  {:bank                      bank/workload
   :large                     large/workload
   :high_availability         (fn [] (println "High Availability workload"))})

(def nemesis-configuration
  "Nemesis configuration"
  {:interval          5
   :kill-node?        true
   :partition-halves? true})

(defn memgraph-ha-test
  "Given an options map from the command line runner constructs a test map for HA tests."
  [opts]
  (merge tests/noop-test
         opts
         {:pure-generators true
          :name            (str "test-" (name (:workload opts)))
          :nodes           (keys (:nodes-config opts))
          :db              (support/db opts)
          :client          (HAClient. nil)}))

(defn memgraph-test
  "Given an options map from the command line runner constructs a test map."
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
                               :exceptions (checker/unhandled-exceptions)
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
  "Resolve all hostnames in config and assign it to the node."
  [nodes-config]

  (info "Resolving hostnames to ip addresses." nodes-config)

  (reduce (fn [curr node]
            (let [k (first node)
                  v (second node)]
              (assoc curr
                     k (assoc v
                              :ip (resolve-hostname k)))))
          {}
          nodes-config))

(defn throw-if-key-missing-in-any
  [map-coll key error-msg]
  (when-not (every? #(contains? % key) map-coll)
    (throw (Exception. error-msg))))

(defn validate-nodes-configuration
  "Validate that configuration of nodes is valid."
  [nodes-config]

  (info "Validating node configuration." nodes-config)

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
   [nil "--nodes-config PATH" "Path to a file containing the config for each node."
    :parse-fn #(-> % e/load-configuration)]])

(defn single-test
  "Takes base CLI options and constructs a single test."
  [opts]
  (let [workload (if (:workload opts)
                   (:workload opts)
                   (throw (Exception. "Workload undefined!")))
        nodes-config (if (:nodes-config opts)
                       (resolve-all-node-hostnames (validate-nodes-configuration (:nodes-config opts)))
                       (throw (Exception. "Nodes config flag undefined!")))
        ; Bank test relies on 100% durable Memgraph, fsyncing after every txn.
        sync-after-n-txn (if (= workload :bank)
                           1
                           100000)
        test-opts (merge opts
                         {:workload workload
                          :nodes-config nodes-config
                          :sync-after-n-txn sync-after-n-txn})]

    (if (= workload :high_availability)
      (memgraph-ha-test test-opts)
      (memgraph-test test-opts))))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn single-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
