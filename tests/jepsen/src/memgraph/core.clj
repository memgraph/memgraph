(ns memgraph.core
  (:gen-class)
  (:require
   [clojure.tools.logging :refer [info]]
   [clojure.edn :as edn]
   [jepsen [cli :as cli]
    [checker :as checker]
    [generator :as gen]
    [history :as history]
    [tests :as tests]]
   [tesser.core :as tesser]
   [memgraph.high-availability.bank.test :as habank]
   [memgraph.high-availability.create.test :as hacreate]
   [memgraph.mtenancy.test :as ha-mt] ; multitenancy + HA test
   [memgraph.replication.bank :as bank]
   [memgraph.replication.large :as large]
   [memgraph.support :as support]))

(defn unhandled-exceptions
  "Wraps jepsen.checker/unhandled-exceptions in a way that if exceptions exist, valid? false is returned.
  Returns information about unhandled exceptions: a sequence of maps sorted in
  descending frequency order, each with:

      :class    The class of the exception thrown
      :count    How many of this exception we observed
      :example  An example operation"
  []
  (reify checker/Checker
    (check [_this _test history _opts]
      (let [exes (->> (tesser/filter history/info?)
                      (tesser/filter :exception)
                      (tesser/group-by (comp :type first :via :exception))
                      (tesser/into [])
                      (history/tesser history)
                      vals
                      (sort-by count)
                      reverse
                      (map (fn [ops]
                             (let [op (first ops)
                                   e  (:exception op)]
                               {:count (count ops)
                                :class (-> e :via first :type)
                                :example op}))))]
        (if (seq exes)
          {:valid?      false
           :exceptions  exes}
          {:valid? true})))))

(defn load-configuration
  "Load edn configuration file."
  [path]
  (-> path slurp edn/read-string))

(def workloads
  "A map of workload names to functions that can take opts and construct
   workloads."
  {:bank                      bank/workload
   :large                     large/workload
   :habank                    habank/workload
   :hacreate                  hacreate/workload
   :ha-mt                     ha-mt/workload})

(defn compose-gen
  "Composes final generator used in the test from client generator and nemesis generator."
  [time-limit client-generator nemesis-generator]
  (gen/time-limit time-limit
                  (gen/nemesis nemesis-generator client-generator)))

(defn memgraph-test
  "Given an options map from the command line runner constructs a test map."
  [opts]
  (let [time-limit (:time-limit opts)
        workload (((:workload opts) workloads) opts)
        client-generator (:generator workload)
        nemesis-config (:nemesis-config workload)
        nemesis-generator (:generator nemesis-config)
        gen (compose-gen time-limit client-generator nemesis-generator)
        ; If final generator exists in the workload, then modify gen, otherwise use what you already have.
        gen      (if-let [final-generator (:final-generator workload)]
                   (gen/phases gen
                               (gen/log "Healing cluster.")
                               (gen/nemesis (:final-generator nemesis-config))
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
                               :exceptions (unhandled-exceptions)
                               :log-checker (checker/log-file-pattern #"[Aa]ssert*|Segmentation fault|core dumped|critical|NullPointerException|json.exception.parse_error|Message response was of unexpected type|Received malformed message from cluster|There is still leftover data in the SLK stream|Failed to close fd" "memgraph.log")
                               :workload   (:checker workload)})
            :nodes           (keys (:nodes-config opts))
            :nemesis         (:nemesis nemesis-config)
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
                       (if (or (= workload :habank) (= workload :hacreate) (= workload :ha-mt))
                         (:nodes-config opts)
                         (validate-nodes-configuration (:nodes-config opts))) ; validate only for replication tests.
                       (throw (Exception. "Nodes config flag undefined!")))
        ; Bank test relies on 100% durable Memgraph, fsyncing after every txn.
        sync-after-n-txn (if (or (= workload :bank) (= workload :habank))
                           1
                           100000)
        licence (when (:license opts)
                  (:license opts))
        organization (when (:organization opts)
                       (:organization opts))
        num-tenants (when (:num-tenants opts)
                      (Integer/parseInt (:num-tenants opts)))

        recovery-time (when (:recovery-time opts)
                        (Integer/parseInt (:recovery-time opts)))

        nemesis-start-sleep (when (:nemesis-start-sleep opts)
                              (Integer/parseInt (:nemesis-start-sleep opts)))
        test-opts (merge opts
                         {:workload workload
                          :nodes-config nodes-config
                          :sync-after-n-txn sync-after-n-txn
                          :license licence
                          :organization organization
                          :num-tenants num-tenants
                          :recovery-time recovery-time
                          :nemesis-start-sleep nemesis-start-sleep})]

    (memgraph-test test-opts)))

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
    :parse-fn #(-> % load-configuration)]
   ["-nt" "--num-tenants NUMBER" "Number of tenants that will be used in multi-tenant env." :default nil]
   ["-rt" "--recovery-time SECONDS" "Recovery time before calling final generator." :default nil]
   ["-nss" "--nemesis-start-sleep SECONDS" "The number of seconds nemesis will sleep before starting its disruptions." :default nil]])

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn single-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
