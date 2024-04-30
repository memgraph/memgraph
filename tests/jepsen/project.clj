(defproject jepsen.memgraph "0.1.0-SNAPSHOT"
  :description "A Jepsen test for Memgraph"
  :url "https://memgraph.com/"
  :license {:name "Memgraph Enterprise"
            :url "https://github.com/memgraph/memgraph/blob/master/release/LICENSE_ENTERPRISE.md"}
  :main jepsen.memgraph.core
  :dependencies [[org.clojure/clojure "1.11.2"]
                 ;; Details under https://clojars.org/jepsen/versions.
                 [jepsen "0.3.5-SNAPSHOT"]
                 [gorillalabs/neo4j-clj "5.0.0"]]

  :pom-addition [:developers
                 [:developer
                  [:id "as51340"]
                  [:name "Andi Skrgat"]
                  [:url "https://github.com/as51340"]
                  [:roles
                   [:role "developer"]
                   [:role "maintainer"]]]]

  :profiles {:test {:dependencies [#_[org.neo4j.test/neo4j-harness "5.10.0"]]}}
  ;; The below line is required to run after Jepsen 0.3.0.
  :aot :all
  :repl-options {:init-ns jepsen.memgraph.core})
