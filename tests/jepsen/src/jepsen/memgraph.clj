(ns jepsen.memgraph
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
                    [control :as c]
                    [db :as db]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(defn db
  "Memgraph DB for a particular version."
  [version binary]
  (reify db/DB
    (setup! [_ test node]
      (if (nil? binary)
		(info node "installing Memgraph" version)
        (info node "installing Memgraph" binary)))

    (teardown! [_ test node]
      (info node "tearing down Memgraph"))))

(defn memgraph-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "memgraph"
          :db   (db "v1.2.0" "build/memgraph-1.3-alpha")
          :pure-generators true}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn memgraph-test})
                   (cli/serve-cmd))
            args))
