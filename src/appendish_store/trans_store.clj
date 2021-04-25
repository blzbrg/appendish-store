(ns appendish-store.trans-store
  (:require [appendish-store.order-blocks :as order-blocks]))

;; === Unsorted storage ===

(defn append-to-unsorted
  [unsorted item]
  (conj unsorted item)) ; assumption: vector

(defn unsorted-over-thresshold?
  [{unsorted ::unsorted threshhold ::full-threshhold}]
  (> (count unsorted) threshhold))

(defn unsorted-full?
  "If ::full-pred is present use it, otherwise use unsorted-over-thresshold? as pred."
  [{pred ::full-pred :as ingress}]
  ((or pred unsorted-over-thresshold?) ingress))

;; === Block storage ===

(defn add-unsorted-as-block
  [sorted-blocks unsorted]
  (conj sorted-blocks (order-blocks/unsorted->block unsorted)))

;; === Input ===

(defn input
  [ingress item]
  ;; This is safe because we only use commute when we are ONLY going to append to unsorted. If we
  ;; used commute and alter in the same transaction we would run the risk of missing additions to
  ;; the unsorted done by other transactions.
  (dosync
   ;; Note that unsorted-full? is applied to the unsorted _without_ the new item. This is purely
   ;; to keep the transaction simpler/lighter.
   (if (unsorted-full? @ingress)
     ;; Make current unsorted plus new item into a new block
     (let [new-unsorted (append-to-unsorted (::unsorted @ingress) item)]
       (alter ingress assoc ::unsorted [])
       (alter (::blocks-ref @ingress) add-unsorted-as-block new-unsorted))
     ;; Append to the current unsorted. Commute allows this transaction to still commit even if
     ;; another changed it in the meantime. This should allow many concurrent inputs to run
     ;; more-concurrently.
     (commute ingress update ::unsorted append-to-unsorted item))))

;; === Block combining ===

(defn should-combine?
  [blocks]
  (> (count blocks) 2))

(defn combine-first-two
  [[block1 block2 & rest]]
  (apply vector (order-blocks/merge-blocks block1 block2) rest))

(defn maybe-combine-blocks
  [blocks]
  (if (should-combine? blocks)
    (recur (combine-first-two blocks))
    blocks))

(defn maybe-combine-watcher
  [_ sorted-ref old new]
  ;; We do not use the old state or new state besides to ensure that we are not triggering an
  ;; endless loop of watchers (each watcher triggers another instance of the same watcher)
  (if (not (= old new))
    (dosync (alter sorted-ref maybe-combine-blocks))))

;; === Initialization ===

(defn initialize
  ([] (initialize {}))
  ([{unsorted-full-pred :unsorted-full-pred unsorted-full-thresh :unsorted-full-threshhold]
   (let [sorted-blocks (ref {::blocks []})
         ingress-opts (cond-> {::full-threshhold (or unsorted-full-thresh 200)}
                        unsorted-full-pred (assoc ::full-pred unsorted-full-pred))]
     (add-watch sorted-blocks ::maybe-combine-watcher maybe-combine-watcher)
     ;; non-namespace symbols since this is only to communicate to callers
     {:ingress (ref (merge {::blocks-ref sorted-blocks
                            ::unsorted []}
                           ingress-opts))
      :sorted-blocks sorted-blocks})))
