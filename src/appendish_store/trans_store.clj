(ns appendish-store.trans-store
  (:require [appendish-store.order-blocks :as order-blocks]))

;; === Unsorted storage ===

(defn append-to-unsorted
  [unsorted item]
  (conj unsorted item)) ; assumption: vector

(defn unsorted-over-thresshold?
  [{unsorted ::unsorted threshhold ::full-threshhold} next-item]
  (> (inc (count unsorted)) threshhold))

(defn unsorted-would-be-full?
  "Check if the unsorted would be full after adding `next-item`. If ::full-pred is present use it,
  otherwise use unsorted-over-thresshold? as pred."
  [{pred ::full-pred :as ingress} next-item]
  ((or pred unsorted-over-thresshold?) ingress next-item))

;; === Block storage ===

(defn add-unsorted-as-block
  [sorted-blocks unsorted]
  (conj sorted-blocks (order-blocks/unsorted->block unsorted)))

(defn block-count
  [{blocks ::blocks}]
  (count blocks))

;; === Input ===

(defn input
  [ingress item]
  ;; This is safe because we only use commute when we are ONLY going to append to unsorted. If we
  ;; used commute and alter in the same transaction we would run the risk of missing additions to
  ;; the unsorted done by other transactions.
  (dosync
   (if (unsorted-would-be-full? @ingress item)
     ;; Make current unsorted plus new item into a new block
     (let [new-unsorted (append-to-unsorted (::unsorted @ingress) item)]
       (alter ingress assoc ::unsorted [])
       (alter (::blocks-ref @ingress) update ::blocks add-unsorted-as-block new-unsorted))
     ;; Append to the current unsorted. Commute allows this transaction to still commit even if
     ;; another changed it in the meantime. This should allow many concurrent inputs to run
     ;; more-concurrently.
     (commute ingress update ::unsorted append-to-unsorted item))))

;; === Block combining ===

(defn blocks-over-threshhold?
  [{thresh ::merge-threshhold :as block-store}]
  (> (block-count block-store) thresh))

(defn should-combine?
  [{pred ::merge-pred :as block-store}]
  ((or pred blocks-over-threshhold?) block-store))

(defn combine-first-two
  [[block1 block2 & rest]]
  (apply vector (order-blocks/merge-blocks block1 block2) rest))

(defn maybe-combine-blocks
  [block-store]
  (if (should-combine? block-store)
    (recur (update block-store ::blocks combine-first-two))
    block-store))

(defn maybe-combine-watcher
  [_ block-store-ref old new]
  ;; We do not use the old state or new state besides to ensure that we are not triggering an
  ;; endless loop of watchers (each watcher triggers another instance of the same watcher)
  (if (not (= old new))
    (dosync (alter block-store-ref maybe-combine-blocks))))

;; === Initialization ===

(defn initialize
  ([] (initialize {}))
  ([{unsorted-full-pred :unsorted-full-pred unsorted-full-thresh :unsorted-full-threshhold
     block-merge-pred :block-merge-pred block-merge-thresh :block-merge-thresh}]
   (let [block-store-ref (ref (cond-> {::merge-threshhold (or block-merge-thresh 2)
                                       ::blocks []}
                                block-merge-pred (assoc ::merge-pred block-merge-pred)))
         ingress-ref (ref (cond-> {::full-threshhold (or unsorted-full-thresh 200)
                                   ::blocks-ref block-store-ref
                                   ::unsorted []}
                            unsorted-full-pred (assoc ::full-pred unsorted-full-pred)))]
     (add-watch block-store-ref ::maybe-combine-watcher maybe-combine-watcher)
     ;; non-namespace symbols since this is only to communicate to callers
     {:ingress ingress-ref :sorted-blocks block-store-ref})))