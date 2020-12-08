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
             [nemesis :as nemesis]
             [tests :as tests]
             [util :as util :refer [meh]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.net URI)))

;; Jepsen related utils.
(defn instance-url
  "An URL for connecting to an instance on a particular port"
  [node port]
  (str "bolt://" node ":" port))

;; neo4j-clj related utils.
(defmacro with-session
  "Execute body expressions by using the same session. Useful when executing
  multiple queries, each as a separete transaction."
  [connection session & body]
  `(with-open [~session (dbclient/get-session ~connection)]
     ~@body))

;; Memgraph database config and setup.
(def mgdir  "/opt/memgraph")
(def mgdata (str mgdir "/mg_data"))
(def mglog  (str mgdir "/memgraph.log"))
(def mgpid  (str mgdir "/memgraph.pid"))
(defn db
  "Manage Memgraph DB on each node."
  [package-url local-binary]
  (reify db/DB
    (setup! [_ test node]
      (c/su (debian/install ['python3 'python3-dev]))
      (c/su (meh (c/exec :killall :memgraph)))
      (when-not (nil? package-url)
        (throw (Exception. "Memgraph package-url setup not yet implemented.")))
      (when (nil? local-binary)
        (throw (Exception. "Memgraph local-binary has to be defined.")))
      (try (c/exec :command :-v local-binary)
           (catch Exception e
             (throw (Exception. (str local-binary " is not there.")))))
      (info node "Memgraph binary is there" local-binary)
      (cu/start-daemon!
       {:logfile mglog
        :pidfile mgpid
        :chdir   mgdir}
       local-binary)
      (Thread/sleep 2000))
    (teardown! [_ test node]
      (info node "Tearing down Memgraph")
      (when (and local-binary mgpid) (cu/stop-daemon! local-binary mgpid))
      (c/su
       (c/exec :rm :-rf mgdata)
       (c/exec :rm :-rf mglog)))
    db/LogFiles
    (log-files [_ test node]
      [mglog])))

;; Abstract operations executed against the tested system.
(defn r [_ _]   {:type :invoke, :f :read,  :value nil})
(defn w [_ _]   {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas,   :value [(rand-int 5) (rand-int 5)]})

;; Operations specific to the tested system (Bolt and Cypher).
(dbclient/defquery create-node
  "CREATE (n:Node {id: $id, value: $value});")
(dbclient/defquery get-node
  "MATCH (n:Node {id: $id}) RETURN n;")
(dbclient/defquery get-all-nodes
  "MATCH (n:Node) RETURN n;")
(dbclient/defquery update-node
  "MATCH (n:Node {id: $id}) SET n.value = $value;")
(defn compare-and-set-node
  [conn o n]
  (dbclient/with-transaction conn tx
    (if (= (-> (get-node tx {:id "0"}) first :n :value) (str o))
      (update-node tx {:id "0" :value n})
      (throw+ "Unable to alter something that does NOT exist."))))
(dbclient/defquery detach-delete-all
  "MATCH (n) DETACH DELETE n;")

;; Client specific to the tested system (Bolt and Cypher).
;; TODO (gitbuda): Write an impl targeting Memgraph replicated cluster.
(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (dbclient/connect (URI. (instance-url node 7687)) "" "")))
  (setup! [this test]
    (with-session conn session
      (create-node session {:id "0" :value 0})))
  (invoke! [this test op]
    (case (:f op)
      :read (assoc op :type :ok
                   :value (with-session conn session
                            (-> (get-node session {:id "0"}) first :n :value)))
      :write (do (with-session conn session
                   (update-node session {:id "0" :value (:value op)}))
                 (assoc op :type :ok))
      :cas (try+
            (let [[o n] (:value op)]
              (assoc op :type (if (compare-and-set-node conn o n) :ok :fail)))
            (catch Object _
              (assoc op :type :fail, :error :not-found)))))
  (teardown! [this test]
    (with-session conn session
      (detach-delete-all session)))
  (close! [_ est]
    (dbclient/disconnect conn)))

(defn memgraph-basic-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:pure-generators true
          :name            "memgraph"
          :db              (db (:package-url  opts) (:local-binary opts))
          :client          (Client. nil)
          :checker         (checker/compose
                             ;; Fails on a cluster of independent Memgraphs.
                            {:linear (checker/linearizable
                                      {:model     (model/cas-register 0)
                                       :algorithm :linear})
                             :perf   (checker/perf)
                             :timeline (timeline/html)})
          :nemesis         (nemesis/partition-random-halves)
          :generator       (->> (gen/mix [r w cas])
                                (gen/stagger 1/50)
                                (gen/nemesis
                                 (cycle [(gen/sleep 5)
                                         {:type :info, :f :start}
                                         (gen/sleep 5)
                                         {:type :info, :f :stop}]))
                                (gen/time-limit (:time-limit opts)))}))

(def cli-opts
  "CLI options for tests."
  [[nil "--package-url URL" "What package of Memgraph should we test?"
    :default nil]
   [nil "--local-binary PATH" "Ignore package; use this local binary instead."
    :default "/opt/memgraph/memgraph"]])

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn memgraph-basic-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
