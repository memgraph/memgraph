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
             [client :as client]
             [history :as h]
             [util :as util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.checker.perf :as perf]
            [jepsen.memgraph.haclient :as haclient]
            [jepsen.memgraph.utils :as utils]))

(defrecord Client [nodes-config license organization]
  client/Client
  (open! [this _ node]
    (info "Opening connection to node" node)
    (let [connection (utils/open-bolt node)
          node-config (get nodes-config node)]
      (assoc this
             :conn connection
             :node-config node-config
             :node node)))
  (setup! [this _]
    (utils/with-session (:conn this) session
      ((haclient/set-db-setting "enterprise.license" license) session)
      ((haclient/set-db-setting "organization.name" organization) session)))

  (invoke! [this _ op]
    (let [node-config (:node-config this)]
      (case (:f op)
        :read (if (contains? node-config :coordinator-id)
                (assoc op
                       :type :ok
                       :value (:coordinator-id node-config))
                (assoc op :type :fail :value "Node is not a coordinator"))

        :register (if (= (:node this) "n4") ; Node with coordinator-id = 1
                    (do
                                             ; Register replication instances
                      (doseq [repl-config (filter #(contains? (val %) :replication-port)
                                                  nodes-config)]
                        (try
                          (utils/with-session (:conn this) session
                            ((haclient/register-replication-instance
                              (first repl-config)
                              (second repl-config)) session))
                          (catch Exception e
                            (assoc op :type :fail :value e))))
                                             ; Add coordinator instances
                      (doseq [coord-config (filter #(contains? (val %) :coordinator-port)
                                                   nodes-config)]
                        (try
                          (utils/with-session (:conn this) session
                            ((haclient/add-coordinator-instance
                              (first coord-config)
                              (second coord-config)) session))
                          (catch Exception e
                            (assoc op :type :fail :value e))))

                      (assoc op :type :ok))
                    (assoc op :type :fail :value "Trying to register on node != n4")))))

  (teardown! [this test])
  (close! [this test]
    (dbclient/disconnect (:conn this))))

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
  {:client    (Client. (:nodes-config opts) (:license opts) (:organization opts))
   :checker   (checker/compose
               {:bank     (haempty-checker)
                :timeline (timeline/html)})
   :generator (haclient/ha-gen (gen/mix [reads]))
   :final-generator {:clients (gen/once reads) :recovery-time 20}})
