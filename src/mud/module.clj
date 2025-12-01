(ns mud.module
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.ops :as ops]
            [com.rpl.rama.aggs :as ags]
            [clojure.string :as str]))

(defrecord Transfer [transfer-id from-user-id to-user-id amt])
(defrecord Deposit [user-id amt])

(defmodule BankDemo
  [setup topologies]
  (declare-depot setup *transfer-depot (hash-by :from-user-id))
  (declare-depot setup *deposit-depot (hash-by :user-id))
  (let [mb (microbatch-topology topologies "banking")]
    (declare-pstate mb $$funds {Long Long})
    (declare-pstate mb $$outgoing-transfers
                    {Long ; user-id
                     (map-schema String ; transfer-id
                                 (fixed-keys-schema {:to-user-id Long
                                                     :amt Long
                                                     :success? Boolean})
                                 {:subindex? true})})
    (declare-pstate mb $$incoming-transfers
                    {Long ; user-id
                     (map-schema String ; transfer-id
                                 (fixed-keys-schema {:from-user-id Long
                                                     :amt Long
                                                     :success? Boolean})
                                 {:subindex? true})})
    (<<sources mb
               (source> *transfer-depot :> %microbatch)
               (%microbatch :> {:keys [*transfer-id *from-user-id *to-user-id *amt]})
               (local-select> [(keypath *from-user-id) (nil->val 0)] $$funds :> *funds)
               (>= *funds *amt :> *success?)
               (<<if *success?
                     (<<ramafn %deduct [*curr]
                               (:> (- *curr *amt)))
                     (local-transform> [(keypath *from-user-id) (term %deduct)] $$funds))
               (local-transform> [(keypath *from-user-id *transfer-id)
                                  (termval {:to-user-id *to-user-id
                                            :amt *amt
                                            :success? *success?})]
                                 $$outgoing-transfers)
               (|hash *to-user-id)
               (<<if *success?
                     (+compound $$funds {*to-user-id (ags/+sum *amt)}))
               (local-transform> [(keypath *to-user-id *transfer-id)
                                  (termval {:from-user-id *from-user-id
                                            :amt *amt
                                            :success? *success?})]
                                 $$incoming-transfers)
               (source> *deposit-depot :> %microbatch)
               (%microbatch :> {:keys [*user-id *amt]})

               (+compound $$funds {*user-id (ags/+sum *amt)}))

    (<<query-topology topologies "money-gone"
                      [*user-id :> *transfer-sum]
                      (|hash *user-id)
                      (local-select> [(keypath *user-id) MAP-VALS :amt] $$outgoing-transfers :> *transfers)
                      (|origin)
                      (ags/+sum *transfers :> *transfer-sum))

    (<<query-topology topologies "money-received"
                      [*user-id :> *transfer-sum]
                      (|hash *user-id)
                      (local-select> [(keypath *user-id) MAP-VALS :amt] $$incoming-transfers :> *transfers)
                      (|origin)
                      (ags/+sum *transfers :> *transfer-sum))

;;     (<<query-topology topologies "dummy"
;;                       [*user-id :> *res]
;;                       (|hash *user-id)
;;                       (+ 100 *user-id :> *res)
;;                       (|origin))

;;     (<<query-topology topologies "transfers-count"
;;                       [*user-id :> *transfer-count]
;;                       (|hash *user-id)

;;                       (local-select> [(keypath *user-id) MAP-KEYS]
;;                                      $$incoming-transfers :> <incoming> *v)

;;                       (gen>)
;;                       (|hash *user-id)
;;                       (local-select> [(keypath *user-id) MAP-KEYS]
;;                                      $$outgoing-transfers :> <outgoing> *v)

;;                       (unify> <incoming> <outgoing>)
;;                       (|origin)
;;                       (ags/+count :> *transfer-count))
    ))
