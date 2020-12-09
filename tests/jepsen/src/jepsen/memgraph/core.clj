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
                             [support :as s]]))

(def workloads
  "A map of workload names to functions that can take opts and construct
   workloads."
  {:basic basic/workload})

(defn memgraph-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)]
    (merge tests/noop-test
           opts
           {:pure-generators true
            :name            (str "Running " (name (:workload opts)))
            :db              (s/db (:package-url opts) (:local-binary opts))
            :client          (:client workload)
            :checker         (checker/compose
                               ;; Fails on a cluster of independent Memgraphs.
                               {;; :stats      (checker/stats) CAS always fails
                                ;;                             so enable this
                                ;;                             if all test have
                                ;;                             at least 1 ok op
                                :exceptions (checker/unhandled-exceptions)
                                :perf       (checker/perf)
                                :workload   (:checker workload)})
            :nemesis         (nemesis/partition-random-halves)
            :generator       (->> (:generator workload)
                                  (gen/nemesis
                                   (cycle [(gen/sleep 5)
                                           {:type :info, :f :start}
                                           (gen/sleep 5)
                                           {:type :info, :f :stop}]))
                                  (gen/time-limit (:time-limit opts)))})))

(def cli-opts
  "CLI options for tests."
  [[nil "--package-url URL" "What package of Memgraph should we test?"
    :default nil]
   [nil "--local-binary PATH" "Ignore package; use this local binary instead."
    :default "/opt/memgraph/memgraph"]
   ["-w" "--workload NAME" "Test workload to run"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]])

(defn all-tests
  "Takes base CLI options and constructs a sequence of test options."
  [opts]
  (let [counts    (range (:test-count opts))
        workloads (if-let [w (:workload opts)] [w] (keys workloads))
        test-opts (for [i counts, w workloads]
                   (assoc opts
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
