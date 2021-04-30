(ns appendish-store.test-lib
  (:require [appendish-store.order-blocks :as order-blocks]))

(defrecord KeyOnly [k]
  order-blocks/KeyedItem
  (order-key [_] k))

(defn keys->items
  [keys]
  (map ->KeyOnly keys))

(defn seq=
  [a b]
  (= (seq a) (seq b)))