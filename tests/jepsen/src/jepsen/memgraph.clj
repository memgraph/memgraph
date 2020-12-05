(ns jepsen.memgraph
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [neo4j-clj.core :as dbclient]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian])
  (:import (java.net URI)))

(def dbdir   "/opt/memgraph")
(def logfile (str dbdir "/memgraph.log"))
(def pidfile (str dbdir "/memgraph.pid"))

(defn db
  "Manage Memgraph DB on each node."
  [package-url local-binary]
  (reify db/DB
    (setup! [_ test node]
      (c/su (debian/install ['python3 'python3-dev]))
      (when (not (nil? package-url))
        (throw (Exception. "Memgraph package-url setup not yet implemented.")))
      (when (nil? local-binary)
        (throw (Exception. "Memgraph local-binary has to be defined.")))
      (when (try (c/exec :command :-v local-binary)
              (catch Exception e
                (throw (Exception. (str local-binary " is not there.")))))
        (info node "Memgraph binary is there" local-binary)
        (cu/start-daemon!
          {:logfile logfile
           :pidfile pidfile
           :chdir   dbdir}
          local-binary)
        (Thread/sleep 2000)))
    (teardown! [_ test node]
      (info node "Tearing down Memgraph")
      (when (and local-binary pidfile) (cu/stop-daemon! local-binary pidfile)))
    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn instance-url
  "An URL for connecting to an instance on a particular port"
  [node port]
  (str "bolt://" node ":" port))

(dbclient/defquery create-node
  "CREATE (n:Node {id: $id});")

(dbclient/defquery get-all-nodes
  "MATCH (n:Node) RETURN n;")

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (dbclient/connect (URI. (instance-url node 7687)) "" "")))
  (setup! [this test]
    (with-open [session (dbclient/get-session conn)]
      (create-node session {:id "1"})))
  (invoke! [this test op]
    (case (:f op)
      :read (assoc op :type :ok,
                      :value (with-open [session (dbclient/get-session conn)]
                               (count (get-all-nodes session))))))
  (teardown! [this test]
    (dbclient/disconnect conn))
  (close! [_ est]))

(defn r [_ _] {:type :invoke, :f :read, :value nil})

(defn memgraph-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:pure-generators true
          :name            "memgraph"
          :db              (db (:package-url  opts) (:local-binary opts))
          :client          (Client. nil)
          :checker (checker/compose
                     {:perf   (checker/perf)
                      :timeline (timeline/html)})
          :generator       (->> r
                                (gen/stagger 1)
                                (gen/nemesis nil)
                                (gen/time-limit 10))}))

(def cli-opts
  "CLI options for tests"
  [[nil "--package-url URL" "What package of Memgraph should we test?"
    :default nil]
   [nil "--local-binary PATH" "Ignore package; use this local binary instead."
    :default "/opt/memgraph/memgraph"]])

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn memgraph-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
