(ns jepsen.memgraph.bank-utils
  "TODO, fill"
  (:require [neo4j-clj.core :as dbclient]))

(def account-num
  "Number of accounts to be created"
  5)

(def starting-balance
  "Starting balance of each account"
  400)

(def max-transfer-amount
  20)

; Implicit 1st parameter you need to send is txn. 2nd is id. 3rd balance
(dbclient/defquery create-account
  "CREATE (n:Account {id: $id, balance: $balance});")

; Implicit 1st parameter you need to send is txn.
(dbclient/defquery get-all-accounts
  "MATCH (n:Account) RETURN n;")

; Implicit 1st parameter you need to send is txn. 2nd is id.
(dbclient/defquery get-account
  "MATCH (n:Account {id: $id}) RETURN n;")

; Implicit 1st parameter you need to send is txn. 2nd is id. 3d is amount.
(dbclient/defquery update-balance
  "MATCH (n:Account {id: $id})
   SET n.balance = n.balance + $amount
   RETURN n")

(defn read-balances
  "Read the current state of all accounts"
  [_ _]
  {:type :invoke, :f :read, :value nil})
