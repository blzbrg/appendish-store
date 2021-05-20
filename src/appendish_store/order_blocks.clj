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
  (->RecordSortedBlock (order-key (first sorted))
                       (order-key (last sorted))
                       (vec sorted)))

(defn unsorted->block
  [unsorted]
  (sorted->block (sort-by order-key unsorted)))

(defn within-block
  [block key]
  (and (<= (compare key (upper-bound block)) 0)
       (>= (compare key (lower-bound block)) 0)))

(defn overlapping?
  [block-1 block-2]
  (or (within-block block-1 (lower-bound block-2))
      (within-block block-1 (upper-bound block-2))
      (within-block block-2 (lower-bound block-1))
      (within-block block-2 (upper-bound block-1))))

(defn fully-contained?
  "Return if narrow is completely included in broad. Unlike overlapping, this is NOT commutative."
  [narrow broad]
  (and (within-block broad (lower-bound narrow))
       (within-block broad (upper-bound narrow))))

(defn append-block
  [old-block new-block]
  (->RecordSortedBlock (lower-bound old-block)
                       (upper-bound new-block)
                       (into (items old-block) (items new-block))))

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
          [unchanged-low overlap-from-low] (splitv (items low-block) (lower-bound high-block))
          new-sorted (merge-sorted-by order-key unchanged-low overlap-from-low (items high-block))]
      ;; avoid looking at sorted at all to compute new min and max. The new min and new max are
      ;; always one of the old min or old max (respectively).
      (->RecordSortedBlock (min (lower-bound low-block) (lower-bound high-block))
                           (max (upper-bound low-block) (upper-bound high-block))
                           new-sorted))))

