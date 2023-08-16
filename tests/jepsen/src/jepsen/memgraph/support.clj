(ns jepsen.memgraph.support
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [db :as db]
             [control :as c]
             [util :as util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

;; Memgraph database config and setup.
(def mgdir  "/opt/memgraph")
(def mgdata (str mgdir "/mg_data"))
(def mglog  (str mgdir "/memgraph.log"))
(def mgpid  (str mgdir "/memgraph.pid"))

(defn start-node!
  [test]
  (cu/start-daemon!
   {:logfile mglog
    :pidfile mgpid
    :chdir   mgdir}
   (:local-binary test)
   :--log-level "TRACE"
   :--also-log-to-stderr
   :--storage-recover-on-startup
   :--storage-wal-enabled
   :--storage-snapshot-interval-sec 300
   :--storage-properties-on-edges))

(defn stop-node!
  [test]
  (cu/stop-daemon! (:local-binary test) mgpid))

(defn db
  "Manage Memgraph DB on each node."
  [opts]
  (reify db/DB
    (setup! [_ test node]
      (let [local-binary (:local-binary opts)]
        (c/su (debian/install ['python3 'python3-dev]))
        (c/su (meh (c/exec :killall :memgraph)))
        (try (c/exec :command :-v local-binary)
             (catch Exception e
               (throw (Exception. (str local-binary " is not there.")))))
        (info node "Memgraph binary is there" local-binary)
        (start-node! test)
        (Thread/sleep 5000))) ;; TODO(gitbuda): The sleep after Jepsen starting Memgraph is for sure questionable.
    (teardown! [_ test node]
      (info node "Tearing down Memgraph")
      (stop-node! test)
      (c/su
       (c/exec :rm :-rf mgdata)
       (c/exec :rm :-rf mglog)))
    db/LogFiles
    (log-files [_ test node]
      [mglog])))
