(ns appendish-store.order-blocks)

(def item->key ::key)

(defn sorted->block
  [sorted]
  {::min-key (item->key (first sorted))
   ::max-key (item->key (last sorted))
   ::sorted (vec sorted)})

(defn unsorted->block
  [unsorted]
  (sorted->block (sort-by item->key unsorted)))

(defn within-block
  [block key]
  (and (<= key (::max-key block)) (>= key (::min-key block))))

(defn overlapping?
  [block-1 block-2]
  (or (within-block block-1 (::min-key block-2))
      (within-block block-1 (::max-key block-2))))