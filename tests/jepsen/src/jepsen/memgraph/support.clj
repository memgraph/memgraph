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
       local-binary
       :--storage-recover-on-startup
       :--storage-wal-enabled
       :--storage-snapshot-interval-sec 300
       :--storage-properties-on-edges)
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
