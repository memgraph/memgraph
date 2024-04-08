(ns jepsen.memgraph.support
  (:require
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
  [test _]
  (cu/start-daemon!
   {:logfile mglog
    :pidfile mgpid
    :chdir   mgdir}
   (:local-binary test)
   :--log-level "INFO"
   :--also-log-to-stderr
   :--storage-recover-on-startup
   :--storage-wal-enabled
   :--storage-snapshot-interval-sec 300
   :--replication-restore-state-on-startup
   :--data-recovery-on-startup
   :--storage-properties-on-edges))

(defn start-coordinator-node!
  [test node-config]
  (info "Coordinator id: " (get node-config :coordinator-id))
  (info "Coordinator port: " (get node-config :coordinator-port))
  (cu/start-daemon!
   {:logfile mglog
    :pidfile mgpid
    :chdir   mgdir}
   (:local-binary test)
   :--log-level "TRACE"
   :--experimental-enabled "high-availability"
   :--also-log-to-stderr
   :--storage-recover-on-startup
   :--storage-wal-enabled
   :--storage-snapshot-interval-sec 300
   :--replication-restore-state-on-startup
   :--data-recovery-on-startup
   :--storage-properties-on-edges
   :--coordinator-id (get node-config :coordinator-id)
   :--coordinator-port (get node-config :coordinator-port)))

(defn start-data-node!
  [test node-config]
  (info "Management port: " (get node-config :management-port))
  (cu/start-daemon!
   {:logfile mglog
    :pidfile mgpid
    :chdir   mgdir}
   (:local-binary test)
   :--log-level "TRACE"
   :--experimental-enabled "high-availability"
   :--also-log-to-stderr
   :--storage-recover-on-startup
   :--storage-wal-enabled
   :--storage-snapshot-interval-sec 300
   :--replication-restore-state-on-startup
   :--data-recovery-on-startup
   :--storage-properties-on-edges
   :--management-port (get node-config :management-port)))

(defn stop-node!
  [test _]
  (cu/stop-daemon! (:local-binary test) mgpid))


(defn db
  "Manage Memgraph DB on each node."
  [opts]
  (reify db/DB ; Construct a new object satisfying the Jepsen's DB protocol.
    (setup! [_ test node] ; Each DB must support setup! method.
      (let [local-binary (:local-binary opts)
            node-config (get (:node-config opts) node)]
        (c/su (debian/install ['python3 'python3-dev]))
        (c/su (meh (c/exec :killall :memgraph)))
        (try (c/exec :command :-v local-binary)
             (catch Exception _
               (throw (Exception. (str local-binary " is not there.")))))
        (if (:coordinator-id node-config)
          (start-coordinator-node! test node-config)
          (if (:management-port node-config)
            (start-data-node! test node-config)
            (start-node! test _)))
        (info "Memgraph instance started")
        (Thread/sleep 5000))) ;; TODO(gitbuda): The sleep after Jepsen starting Memgraph is for sure questionable.
    (teardown! [_ test node] ; Each DB must support teardown! method.
      (info node "Tearing down Memgraph")
      (stop-node! test _)
      (c/su
       (c/exec :rm :-rf mgdata)
       (c/exec :rm :-rf mglog)))
    db/LogFiles
    (log-files [_ _ _]
      [mglog])))
