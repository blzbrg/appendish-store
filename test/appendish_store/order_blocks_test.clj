(ns appendish-store.order-blocks-test
  (:require [clojure.test :as test]
            [appendish-store.test-lib :refer [keys->items seq=]]
            [appendish-store.order-blocks :as order-blocks]))

(test/deftest unsorted->block-test
  (test/is (= (order-blocks/unsorted->block (keys->items [1 3 2]))
         {::order-blocks/min-key 1
          ::order-blocks/max-key 3
          ::order-blocks/sorted (keys->items [1 2 3])})))

(test/deftest within-block-test
  (let [b (order-blocks/unsorted->block (keys->items [1 3 2]))]
    (test/is (order-blocks/within-block b 1))
    (test/is (order-blocks/within-block b 2))
    (test/is (order-blocks/within-block b 3))
    (test/is (not (order-blocks/within-block b 4)))
    (test/is (not (order-blocks/within-block b 0)))))

(test/deftest test-overlapping
  (let [b1 (order-blocks/unsorted->block (keys->items [1 2 3]))
        b2 (order-blocks/unsorted->block (keys->items [2 3 4]))
        b3 (order-blocks/unsorted->block (keys->items [3 4 5]))
        db (order-blocks/unsorted->block (keys->items [7 8 9]))]
    (test/is (order-blocks/overlapping? b1 b1))
    (test/is (order-blocks/overlapping? b1 b2))
    (test/is (order-blocks/overlapping? b1 b3))
    (test/is (order-blocks/overlapping? b2 b3))
    (test/is (not (order-blocks/overlapping? b1 db)))))

(test/deftest test-append-block
  (let [b1 (order-blocks/sorted->block (keys->items [1 2 3]))
        b2 (order-blocks/sorted->block (keys->items [4 5 6]))]
    (test/is (= (order-blocks/append-block b1 b2)
                (order-blocks/sorted->block (keys->items [1 2 3 4 5 6]))))))

(test/deftest test-merge-sorted-by
  (let [a (keys->items [1 2 4])
        b (keys->items [3 5])]
    (test/is (seq= (order-blocks/merge-sorted-by order-blocks/item->key a b)
                   (keys->items [1 2 3 4 5])))
    (test/is (seq= (order-blocks/merge-sorted-by order-blocks/item->key a [])
                   a))
    (test/is (seq= (order-blocks/merge-sorted-by order-blocks/item->key b a)
                   (keys->items [1 2 3 4 5])))))

(test/deftest test-merge-blocks
  (let [b1 (order-blocks/sorted->block (keys->items [1 2 4]))
        b2 (order-blocks/sorted->block (keys->items [3 5 6]))
        b12 (order-blocks/sorted->block (keys->items [1 2 3 4 5 6]))
        b3 (order-blocks/sorted->block (keys->items [5 6 7]))]
    (test/is (= (order-blocks/merge-blocks b1 b2) b12))
    (test/is (= (order-blocks/merge-blocks b2 b1) b12))
    (test/is (= (order-blocks/merge-blocks b1 b3)
                (order-blocks/sorted->block (keys->items [1 2 4 5 6 7]))))))
