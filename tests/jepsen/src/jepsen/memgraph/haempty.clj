(ns jepsen.memgraph.haempty
  "TODO: andi Write description"
  (:require [neo4j-clj.core :as dbclient]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as string]
            [clojure.core.reducers :as r]
            [jepsen
             [store :as store]
             [checker :as checker]
             [generator :as gen]
             [history :as h]
             [util :as util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.checker.perf :as perf]
            [jepsen.memgraph.haclient :as haclient]
            [jepsen.memgraph.utils :as utils]))

(haclient/ha-client Client []
                    ; Open connection to the node. Setup each node.
                    (open! [this test node]
                           (haclient/ha-open-connection this node nodes-config))
                           ; On main detach-delete-all and create accounts.
                    (setup! [this test])
                    (invoke! [this test op]
                             (case (:f op)
                               :read (assoc op
                                            :type :ok
                                            :value "PLACEHOLDER")
                               :register (if (= node "n4") ; Node with coordinator-id = 1
                                           (do
                                             (doseq [repl-config (filter #(contains? (val %) :replication-port)
                                                                         nodes-config)]
                                               (try
                                                 (utils/with-session conn session
                                                   ((haclient/register-replication-instance
                                                     (first repl-config)
                                                     (second repl-config)) session))
                                                 (catch Exception e
                                                   (assoc op :type :fail :info e))))
                                             (assoc op :type :ok))
                                           (assoc op :type :fail :info "Trying to register on node != n4"))))

                    (teardown! [this test])
                    ; Close connection to the node.
                    (close! [_ test]
                            (dbclient/disconnect conn)))

(defn reads
  "Current read placeholder."
  [_ _]
  {:type :invoke, :f :read, :value nil})

(defn haempty-checker
  "HA empty checker"
  []
  (reify checker/Checker
    (check [_ _ _ _]
      {:valid? true})))

(defn workload
  "Basic test workload"
  [opts]
  {:client    (Client. nil nil nil (:nodes-config opts))
   :checker   (checker/compose
               {:bank     (haempty-checker)
                :timeline (timeline/html)})
   :generator (haclient/ha-gen (gen/mix [reads]))
   :final-generator {:clients (gen/once reads) :recovery-time 20}})
