(ns jepsen.memgraph
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [neo4j-clj.core :as dbclient]
            [jepsen [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [tests :as tests]]
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
        (Thread/sleep 10000)))
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

(defn peer-url
  "The URL for other peers to talk to an instance"
  [node]
  (instance-url node 7687))

(defn initial-cluster
  "Constructs an initial cluster string for a test"
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (str node "=" (peer-url node))))
       (str/join ",")))

(defrecord Client [conn]
  client/Client
  (open! [this test node] this)
  (setup! [this test])
  (invoke! [this test op])
  (teardown! [this test])
  (close! [_ est]))

(defn memgraph-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "memgraph"
          :db   (db (:package-url  opts) (:local-binary opts))
          :client (Client. nil)
          :pure-generators true}))

(def cli-opts
  "CLI options for tests"
  [[nil "--package-url URL" "What package of Memgraph should we test?"
    :default nil]
   [nil "--local-binary PATH" "Ignore version; use this local binary instead."
    :default "/opt/memgraph/memgraph"]])

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn memgraph-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
