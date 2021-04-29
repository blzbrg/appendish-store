(ns appendish-store.test-lib)

(defn keys->items
  [keys]
  (mapv (fn [key] {:appendish-store.order-blocks/key key}) keys))

(defn seq=
  [a b]
  (= (seq a) (seq b)))