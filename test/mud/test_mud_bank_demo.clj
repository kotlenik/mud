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

          alice-id 0
          bob-id 1
          charlie-id 2]

      (foreign-append! deposit-depot (mm/->Deposit alice-id 200))
      (foreign-append! deposit-depot (mm/->Deposit bob-id 100))
      (foreign-append! deposit-depot (mm/->Deposit charlie-id 100))

      (rtest/wait-for-microbatch-processed-count ipc module-name "banking" 3)

      ;; transfer-id is now auto-generated inside the topology as a UUID v7 string
      (foreign-append! transfer-depot (mm/->Transfer alice-id bob-id 50))
      (foreign-append! transfer-depot (mm/->Transfer alice-id charlie-id 160))
      (foreign-append! transfer-depot (mm/->Transfer alice-id charlie-id 25))
      (foreign-append! transfer-depot (mm/->Transfer charlie-id bob-id 10))

      (rtest/wait-for-microbatch-processed-count ipc module-name "banking" 7)

      ;; Assert on the final funds for each user
      (is (= 125 (foreign-select-one (keypath alice-id) funds)))
      (is (= 160 (foreign-select-one (keypath bob-id) funds)))
      (is (= 115 (foreign-select-one (keypath charlie-id) funds)))

      ;; Verify the outgoing transfers of alice
      ;; Keys are auto-generated UUID v7 strings; check values only
      (let [transfers (foreign-select [(keypath alice-id) ALL] outgoing-transfers)]
        (is (= 3 (count transfers)))
        (is (every? string? (map first transfers)))
        (is (= #{{:to-user-id bob-id     :amt 50  :success? true}
                 {:to-user-id charlie-id :amt 160 :success? false}
                 {:to-user-id charlie-id :amt 25  :success? true}}
               (set (map second transfers)))))

      ;; Verify the outgoing transfers of charlie
      (let [transfers (foreign-select [(keypath charlie-id) ALL] outgoing-transfers)]
        (is (= 1 (count transfers)))
        (is (every? string? (map first transfers)))
        (is (= #{{:to-user-id bob-id :amt 10 :success? true}}
               (set (map second transfers)))))

      ;; Verify the incoming transfers of bob
      (let [transfers (foreign-select [(keypath bob-id) ALL] incoming-transfers)]
        (is (= 2 (count transfers)))
        (is (every? string? (map first transfers)))
        (is (= #{{:from-user-id alice-id   :amt 50 :success? true}
                 {:from-user-id charlie-id :amt 10 :success? true}}
               (set (map second transfers)))))

      ;; Verify the incoming transfers of charlie
      (let [transfers (foreign-select [(keypath charlie-id) ALL] incoming-transfers)]
        (is (= 2 (count transfers)))
        (is (every? string? (map first transfers)))
        (is (= #{{:from-user-id alice-id :amt 160 :success? false}
                 {:from-user-id alice-id :amt 25  :success? true}}
               (set (map second transfers))))))))
