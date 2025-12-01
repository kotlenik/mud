(ns mud.test-mud-bank-demo
  (:use [com.rpl rama]
        [com.rpl.rama path])
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.rpl.rama.test :as rtest]
   [mud.module :as mm]))

(deftest bank-demo-transfer-module-test
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc mm/BankDemo {:tasks 4 :threads 2})
    (let [module-name (get-module-name mm/BankDemo)
          transfer-depot (foreign-depot ipc module-name "*transfer-depot")
          deposit-depot (foreign-depot ipc module-name "*deposit-depot")
          funds (foreign-pstate ipc module-name "$$funds")
          outgoing-transfers (foreign-pstate ipc module-name "$$outgoing-transfers")
          incoming-transfers (foreign-pstate ipc module-name "$$incoming-transfers")

          ;; Declare some constants to make the test code easier to read
          alice-id 0
          bob-id 1
          charlie-id 2]

      (foreign-append! deposit-depot (mm/->Deposit alice-id 200))
      (foreign-append! deposit-depot (mm/->Deposit bob-id 100))
      (foreign-append! deposit-depot (mm/->Deposit charlie-id 100))

      (rtest/wait-for-microbatch-processed-count ipc module-name "banking" 3)

      ;; This transfer will succeed.
      (foreign-append! transfer-depot (mm/->Transfer "alice->bob1" alice-id bob-id 50))
      ;; This transfer will fail because alice has only 150 funds after the first transfer.
      (foreign-append! transfer-depot (mm/->Transfer "alice->charlie1" alice-id charlie-id 160))
      ;; This transfer will succeed.
      (foreign-append! transfer-depot (mm/->Transfer "alice->charlie2" alice-id charlie-id 25))
      ;; This transfer will succeed.
      (foreign-append! transfer-depot (mm/->Transfer "charlie->bob1" charlie-id bob-id 10))

      (rtest/wait-for-microbatch-processed-count ipc module-name "banking" 7)

      ;; Assert on the final funds for each user
      (is (= 125 (foreign-select-one (keypath alice-id) funds)))
      (is (= 160 (foreign-select-one (keypath bob-id) funds)))
      (is (= 115 (foreign-select-one (keypath charlie-id) funds)))

      ;; Verify the outgoing transfers of alice
      (let [transfers (foreign-select [(keypath alice-id) ALL] outgoing-transfers)]
        (is (= 3 (count transfers)))
        (is (= #{["alice->bob1" {:to-user-id bob-id :amt 50 :success? true}]
                 ["alice->charlie1" {:to-user-id charlie-id :amt 160 :success? false}]
                 ["alice->charlie2" {:to-user-id charlie-id :amt 25 :success? true}]}
               (set transfers))))

      ;; Verify the outgoing transfers of charlie
      (let [transfers (foreign-select [(keypath charlie-id) ALL] outgoing-transfers)]
        (is (= 1 (count transfers)))
        (is (= [["charlie->bob1" {:to-user-id bob-id :amt 10 :success? true}]]
               transfers)))

      ;; Verify the incoming transfers of bob
      (let [transfers (foreign-select [(keypath bob-id) ALL] incoming-transfers)]
        (is (= 2 (count transfers)))
        (is (= #{["alice->bob1" {:from-user-id alice-id :amt 50 :success? true}]
                 ["charlie->bob1" {:from-user-id charlie-id :amt 10 :success? true}]}
               (set transfers))))

      ;; Verify the incoming transfers of charlie
      (let [transfers (foreign-select [(keypath charlie-id) ALL] incoming-transfers)]
        (is (= 2 (count transfers)))
        (is (= #{["alice->charlie1" {:from-user-id alice-id :amt 160 :success? false}]
                 ["alice->charlie2" {:from-user-id alice-id :amt 25 :success? true}]}
               (set transfers)))))))
