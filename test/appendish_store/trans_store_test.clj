(ns appendish-store.trans_store_test
  (:require [appendish-store.trans-store :as trans-store]
            [appendish-store.test-lib :refer [keys->items]]
            [clojure.test :as test]))

(defn ingress-items
  [ingress]
  (set (::trans-store/unsorted @ingress)))

(defn blocks-items
  [blocks]
  (apply concat (map :appendish-store.order-blocks/sorted (::trans-store/blocks @blocks))))

(defn add-all
  "Add all items to the given ingress, optionally sleeping after each."
  ([ingress-ref items] (add-all 100 ingress-ref items))
  ([wait-ms ingress-ref items]
   (doseq [inp items]
     (trans-store/input ingress-ref inp)
     (Thread/sleep wait-ms))))

(test/deftest synchronous-inserts
  (let [items (keys->items [1 2 3 5 4 6 7 8 9])
        init-res (trans-store/initialize {:unsorted-full-threshhold 5})
        {in :ingress blocks :sorted-blocks} init-res]
    (add-all 0 in items)
    ;;(doseq [item items] (trans-store/input in item))
    ;; there is one block
    (test/is (= 1 (trans-store/block-count @blocks)))
    ;; number in is number stored
    (test/is (= (count items) (+ (count (ingress-items in)) (count (blocks-items blocks)))))
    ;; all the items put in are in the store
    (test/is (= (set items) (set (concat (ingress-items in) (blocks-items blocks)))))))


(defn all-complete
  [& futs]
  (doseq [fut futs] @fut))

(test/deftest asynchronous-inserts
  (let [items (keys->items [1 2 3 5 4 6 7 8 9])
        init-res (trans-store/initialize {:unsorted-full-threshhold 5})
        {in :ingress blocks :sorted-blocks} init-res
        fut-a (future (add-all in (take 5 items)))
        fut-b (future (add-all in (drop 5 items)))]
    ;; wait for both
    (all-complete fut-a fut-b)
    ;; there is one block
    (test/is (= 1 (trans-store/block-count @blocks)))
    ;; number in is number stored
    (test/is (= (count items) (+ (count (ingress-items in)) (count (blocks-items blocks)))))
    ;; all the items put in are in the store
    (test/is (= (set items) (set (concat (ingress-items in) (blocks-items blocks)))))))

;; test only the pred variant, the threshhold variant is implicitly tested in previous tests.
(test/deftest unsorted-full-pred
  (let [magic (first (keys->items [42]))
        has-magic (fn [{uns ::trans-store/unsorted}] (some (partial = magic) uns))
        {in :ingress} (trans-store/initialize {:unsorted-full-pred has-magic})]
    (trans-store/input in magic)
    (test/is (trans-store/unsorted-full? @in))))

(test/deftest low-threshholds
  (let [items (keys->items [1 2 3 5 4 6 7 8 9 10])
        init-res (trans-store/initialize {:unsorted-full-threshhold 2 :block-merge-thresh 2})
        {in :ingress blocks :sorted-blocks} init-res]
    (add-all 0 in items)
    ;; three blocks initially, but they should be merged down to 2
    (test/is (= 2 (trans-store/block-count @blocks)))
    ;; number in is number stored
    (test/is (= (count items) (+ (count (ingress-items in)) (count (blocks-items blocks)))))
    ;; all the items put in are in the store
    (test/is (= (set items) (set (concat (ingress-items in) (blocks-items blocks)))))))
