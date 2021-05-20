(ns appendish-store.order-blocks)

(defprotocol KeyedItem
  "Interface that items must implement to be stored in order-blocks. Using a protocol allows items to
  call their fields anything they like. In addition to implemeting this protocol items should be
  immutable."
  (order-key [self] "Return key used to order items. Must return the same key every time it is
  called on the same object."))

(defprotocol SortedBlock
  "Public read-only API for a sorted block (the type dealt with by order-blocks)."
  (lower-bound [self] "Get the lower bound of the block. This value is the order-key of one of the items
  in the block.")
  (upper-bound [self] "Get the upper bound of the block. This value is the order-key of one of the items
  in the block")
  (items [self] "Get a vector of the items in the block in order from low to high according to
  order-key. For example, (lower-bound block) is equal to (order-key (first (items block)))."))

(defrecord RecordSortedBlock [min-key max-key sorted]
  SortedBlock
  (lower-bound [_] min-key)
  (upper-bound [_] max-key)
  (items [_] sorted))

(defn sorted->block
  [sorted]
  {::min-key (order-key (first sorted))
   ::max-key (order-key (last sorted))
   ::sorted (vec sorted)})

(defn unsorted->block
  [unsorted]
  (sorted->block (sort-by order-key unsorted)))

(defn within-block
  [block key]
  (and (<= (compare key (::max-key block)) 0)
       (>= (compare key (::min-key block)) 0)))

(defn overlapping?
  [block-1 block-2]
  (or (within-block block-1 (::min-key block-2))
      (within-block block-1 (::max-key block-2))
      (within-block block-2 (::min-key block-1))
      (within-block block-2 (::max-key block-1))))

(defn fully-contained?
  "Return if narrow is completely included in broad. Unlike overlapping, this is NOT commutative."
  [narrow broad]
  (and (within-block broad (::min-key narrow))
       (within-block broad (::max-key narrow))))

(defn append-block
  [old-block new-block]
  {::min-key (::min-key old-block)
   ::max-key (::max-key new-block)
   ::sorted (into (::sorted old-block) (::sorted new-block))})

(defn splitv
  [coll split-key]
  ;; note that zero is truthy, which is important so that we can split at index 0
  (let [maybe-idx (fn [[idx item]] (when (>= (compare (order-key item) split-key) 0) idx))
        ;; this is one pass through the vector
        split-idx (some maybe-idx (map vector (range) coll))]
    ;; subvec is allegedly very fast (conceptually it is slicing)
    [(subvec coll 0 split-idx) (subvec coll split-idx)]))

(defn select-next
  [sort-by coll-a coll-b]
  (let [cmp (compare (sort-by (first coll-a)) (sort-by (first coll-b)))]
    (cond
      (<= cmp 0) [(first coll-a) (next coll-a) coll-b]
      (> cmp 0) [(first coll-b) coll-a (next coll-b)])))

(defn merge-sorted-by
  "Return vector resulting from merging two sorted sequables into the vector dest. (sort-by item) must
  return a Comparable used to order item."
  [sort-by dest coll-a coll-b]
  (loop [new dest ;; use vectors within this function for efficient append
         a coll-a
         b coll-b]
    (cond
      (empty? a) (into new b)
      (empty? b) (into new a)
      :else (let [[n new-a new-b] (select-next sort-by a b)]
              ;; append to vector using conj (will not work for other types)
              (recur (conj new n) new-a new-b)))))

(defn merge-blocks
  [low-block high-block]
  (if (not (overlapping? low-block high-block))
    (append-block low-block high-block)
    (let [; split low to get minimal part which overlaps with high
          [unchanged-low overlap-from-low] (splitv (::sorted low-block) (::min-key high-block))
          new-sorted (merge-sorted-by order-key unchanged-low overlap-from-low (::sorted high-block))]
      ;; avoid looking at sorted at all to compute new min and max. The new min and new max are
      ;; always one of the old min or old max (respectively).
      {::min-key (min (::min-key low-block) (::min-key high-block))
       ::max-key (max (::max-key low-block) (::max-key high-block))
       ::sorted new-sorted})))

