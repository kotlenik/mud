(ns user
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:import [com.rpl.rama RamaClusterManager Path])
  (:require [com.rpl.rama.ops :as ops]
            [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.test :as rtest]
            [clojure.string :as str]
            [mud.module :as mm]))


(defn init-cluster []
  (rtest/create-ipc))

(defn shutdown-cluster [c]
  (close! c))

(defn launch-module [cluster module tasks threads]
  (rtest/launch-module! cluster module {:tasks tasks :threads threads}))

(defn get-depot [cluster module depot-name]
  (foreign-depot cluster (get-module-name module) depot-name))

(defn get-pstate [cluster module pstate-name]
  (foreign-pstate cluster (get-module-name module) pstate-name))

(defn put-to-depot [depot value]
  (foreign-append! depot value))

(defn get-from-pstate [pstate path]
  (foreign-select-one (keypath path) pstate))

(defn create-manager []
  (open-cluster-manager {"conductor.host" "localhost"}))

(defn append-deposit [manager user-id amt]
  (let [module-name (get-module-name mm/BankDemo)
        depot-name "*deposit-depot"
        depot (foreign-depot manager module-name depot-name)]
    (foreign-append! depot (mm/->Deposit user-id amt))))

(defn append-transfer [manager tx-id from-user-id to-user-id amt]
  (let [module-name (get-module-name mm/BankDemo)
        depot-name "*transfer-depot"
        depot (foreign-depot manager module-name depot-name)]
    (foreign-append! depot (mm/->Transfer tx-id from-user-id to-user-id amt))))

(defn read-funds [manager user-id]
  (let [module-name (get-module-name mm/BankDemo)
        pstate-name "$$funds"
        pstate (foreign-pstate manager module-name pstate-name)]
    (foreign-select-one (keypath user-id) pstate)))

(defn read-money-gone [manager user-id]
  (let [module-name (get-module-name mm/BankDemo)
        query-name "money-gone"
        query (foreign-query manager module-name query-name)]
    (foreign-invoke-query query user-id)))

(defn read-money-received [manager user-id]
  (let [module-name (get-module-name mm/BankDemo)
        query-name "money-received"
        query (foreign-query manager module-name query-name)]
    (foreign-invoke-query query user-id)))

(defn read-dummy [manager user-id]
  (let [module-name (get-module-name mm/BankDemo)
        query-name "dummy"
        query (foreign-query manager module-name query-name)]
    (foreign-invoke-query query user-id)))

(defn read-transfers-count [manager user-id]
  (let [module-name (get-module-name mm/BankDemo)
        query-name "transfers-count"
        query (foreign-query manager module-name query-name)]
    (foreign-invoke-query query user-id)))

(defn run-test []
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc mm/BankDemo {:tasks 4 :threads 2})
    (let [module-name (get-module-name mm/BankDemo)
          transfer-depot (foreign-depot ipc module-name "*transfer-depot")
          deposit-depot (foreign-depot ipc module-name "*deposit-depot")
          funds (foreign-pstate ipc module-name "$$funds")
          outgoing-transfers (foreign-pstate ipc module-name "$$outgoing-transfers")
          incoming-transfers (foreign-pstate ipc module-name "$$incoming-transfers")

          ;; Declare some constants to make the test code easier to read
          alice-id 1
          bob-id 2
          charlie-id 3
          dave-id 4

          q-mg (foreign-query ipc module-name "money-gone")
          q-mr (foreign-query ipc module-name "money-received")
          ;; q-tc (foreign-query ipc module-name "transfers-count")
          ;; q-dummy (foreign-query ipc module-name "dummy")
          ]

      (foreign-append! deposit-depot (mm/->Deposit alice-id 800))

      (rtest/wait-for-microbatch-processed-count ipc module-name "banking" 1)

      (foreign-append! transfer-depot (mm/->Transfer "tx1" alice-id bob-id 150))

      (rtest/wait-for-microbatch-processed-count ipc module-name "banking" 2)

      (foreign-append! transfer-depot (mm/->Transfer "tx2" alice-id charlie-id 100))

      (rtest/wait-for-microbatch-processed-count ipc module-name "banking" 3)

      (foreign-append! transfer-depot (mm/->Transfer "tx3" bob-id charlie-id 50))

      (foreign-append! transfer-depot (mm/->Transfer "tx4" charlie-id alice-id 50))

      (foreign-append! transfer-depot (mm/->Transfer "tx5" charlie-id dave-id 10))

      (rtest/wait-for-microbatch-processed-count ipc module-name "banking" 6)

      (println "Alice funds:" (foreign-select-one (keypath alice-id) funds))
      (println "Bob funds:" (foreign-select-one (keypath bob-id) funds))
      (println "Charlie funds:" (foreign-select-one (keypath charlie-id) funds))
      (println "Dave funds:" (foreign-select-one (keypath dave-id) funds))

      (println "A incoming:" (foreign-select [(keypath alice-id) MAP-KEYS] incoming-transfers))
      (println "A outgoing:" (foreign-select [(keypath alice-id) MAP-KEYS] outgoing-transfers))

      (println "B incoming:" (foreign-select [(keypath bob-id) MAP-KEYS] incoming-transfers))
      (println "B outgoing:" (foreign-select [(keypath bob-id) MAP-KEYS] outgoing-transfers))

      (println "C incoming:" (foreign-select [(keypath charlie-id) MAP-KEYS] incoming-transfers))
      (println "C outgoing:" (foreign-select [(keypath charlie-id) MAP-KEYS] outgoing-transfers))

      (println "D incoming:" (foreign-select [(keypath dave-id) MAP-KEYS] incoming-transfers))
      (println "D outgoing:" (foreign-select [(keypath dave-id) MAP-KEYS] outgoing-transfers))

      (println "money gone for A:" (foreign-invoke-query q-mg alice-id))
      (println "money gone for B:" (foreign-invoke-query q-mg bob-id))
      (println "money gone for C:" (foreign-invoke-query q-mg charlie-id))

      (println "money received for A:" (foreign-invoke-query q-mr alice-id))
      (println "money received for B:" (foreign-invoke-query q-mr bob-id))
      (println "money received for C:" (foreign-invoke-query q-mr charlie-id))

      ; (println "transfer count for A:" (foreign-invoke-query q-tc alice-id))
      ; (println "transfer count for B:" (foreign-invoke-query q-tc bob-id))
      ; (println "transfer count for C:" (foreign-invoke-query q-tc charlie-id))
      
      ;; (println "dummy for 15:" (foreign-invoke-query q-dummy 15))

      )))
