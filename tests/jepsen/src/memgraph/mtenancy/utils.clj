(ns memgraph.mtenancy.utils)

(defn generate-db-name
  "Generates db name"
  [id]
  (str "db" id))

(defn get-new-dbs
  "Generates names for all newly created databases, excluding the default database 'memgraph'. Returns num-tenants - 1 names because of the
  excluded default database.
  "
  [num-tenants]
  (let
   [ids (range 1 num-tenants)
    dbs (map generate-db-name ids)]
    dbs))

(defn get-all-dbs
  "Generates names for all databases, including the default database 'memgraph'.
  Returns num-tenants names."
  [num-tenants]
  (let [dbs (get-new-dbs num-tenants)
        dbs (conj dbs "memgraph")]
    dbs))

(defn get-random-db
  "Returns random database from all-dbs."
  [num-tenants]
  (rand-nth (get-all-dbs num-tenants)))
