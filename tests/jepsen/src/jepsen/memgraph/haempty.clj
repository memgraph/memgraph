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
                               :read
                               (assoc op
                                      :type :ok
                                      :value "PLACEHOLDER")))
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
   :generator (haclient/replication-gen (gen/mix [reads])) ; TODO (andi) Try to avoid using replication-gen here
   :final-generator {:clients (gen/once reads) :recovery-time 20}})
