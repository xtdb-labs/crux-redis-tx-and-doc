(ns io.dominic.crux.redis
  (:require [crux.db :as db]
            [crux.document-store :as ds]
            [crux.lru :as lru]
            [crux.system :as sys]
            [crux.io :as cio]
            [crux.tx :as tx]
            [taoensso.nippy :as nippy])
  (:import java.io.Closeable
           java.util.Date
           [java.nio ByteBuffer]
           [redis.clients.jedis Jedis JedisPool StreamEntryID]))

(set! *warn-on-reflection* true)

(def ^:private ^"[B" xadd-gen-id (.getBytes "*" "UTF-8"))
(def ^:private ^"[B" tx-events-key (.getBytes "tx-events" "UTF-8"))

(def ^:private ^"[B" range-max-key (.getBytes "+" "UTF-8"))
(def ^:private ^"[B" range-min-key (.getBytes "-" "UTF-8"))

(defn- stream-entry-id->tx-id
  ^long [^StreamEntryID id]
  (let [shortseq (.shortValue (Long. (.getSequence id)))
        bb (ByteBuffer/allocate Long/BYTES)]
    (.putLong bb (.getTime id))
    (.putShort bb 0 shortseq)
    (.getLong bb 0)))

(defn- tx-id->stream-entry-id
  [^long tx-id]
  (let [bb (ByteBuffer/allocate Long/BYTES)]
    (.putLong bb tx-id)
    (let [shortseq (.getShort bb 0)]
      (.putShort bb 0 0)
      (StreamEntryID.
        (.getLong bb 0)
        shortseq))))

(defn- stream-entry->tx-log-entry
  [^StreamEntryID seid tx-events]
  {::tx/tx-id (stream-entry-id->tx-id seid)
   ::tx/tx-time (Date. (.getTime seid))
   :crux.tx.event/tx-events tx-events})

(defrecord RedisTxLog [^JedisPool jedis-pool ^bytes stream-key-bytes tx-consumer]
  db/TxLog
  (submit-tx [this tx-events]
    (with-open [^Jedis j (.getResource jedis-pool)]
      (let [^bytes bid (let [bb (nippy/freeze tx-events)]
                         (.xadd j
                                stream-key-bytes
                                xadd-gen-id
                                (java.util.Map/copyOf {tx-events-key bb})
                                (count bb)
                                false))
            id (StreamEntryID. (String. bid "UTF-8"))]
        (delay
          {::tx/tx-id (stream-entry-id->tx-id id)
           ::tx/tx-time (Date. (.getTime id))}))))

  (open-tx-log [this after-tx-id]
    (cio/->cursor
      (constantly false)
      (map (fn [[^bytes id [_tx-events-key ^bytes tx-events]]]
             (stream-entry->tx-log-entry
               (StreamEntryID. (String. id "UTF-8"))
               (nippy/thaw tx-events)))
           (-> (with-open [^Jedis j (Jedis.)]
                 (.xread j 100 10000
                         ^java.util.Map
                         (hash-map stream-key-bytes
                                   (if after-tx-id
                                     (.getBytes
                                       (str (tx-id->stream-entry-id after-tx-id))
                                       "UTF-8")
                                     (.getBytes "0-0" "UTF-8")))))
               ;; Get the stream we're following - there's only one
               first
               ;; And the stream entries from that stream (discard the name)
               second))))

  (latest-submitted-tx [this]
    (with-open [^Jedis j (.getResource jedis-pool)]
      {:crux.tx/tx-id
       (let [^bytes id (ffirst
                         (.xrevrange
                           j
                           stream-key-bytes
                           range-max-key
                           range-min-key
                           1))]
         (stream-entry-id->tx-id (StreamEntryID. (String. id "UTF-8"))))}))

  Closeable
  (close [_]
    (cio/try-close tx-consumer)))

(defn- ->jedis-pool {::sys/args {:host {:doc "Host to connect to"
                                        :spec ::sys/string
                                        :required? true}
                                 :port {:doc "Port to connect to"
                                        :spec ::sys/int
                                        :default 6379
                                        :required? true}}}
  [{:keys [^String host ^long port]}]
  ;; TODO: Way more ways to connect with Jedis:
  ;; * All arities
  ;; * Clustering
  ;; * Pool configs
  (JedisPool. host port))

(defn ->ingest-only-tx-log {::sys/deps {:jedis-pool `->jedis-pool}
                            ::sys/args {:stream-key {:spec ::sys/string
                                                     :required? true
                                                     :doc "Key to use for stream"
                                                     :default "crux-transaction-log"}}}
  [{:keys [jedis-pool ^String stream-key]}]
  (map->RedisTxLog {:jedis-pool jedis-pool
                    :stream-key-bytes (.getBytes stream-key "UTF-8")}))

(defn ->tx-log {::sys/deps (merge (::sys/deps (meta #'tx/->polling-tx-consumer))
                                  (::sys/deps (meta #'->ingest-only-tx-log)))
                ::sys/args (merge (::sys/args (meta #'tx/->polling-tx-consumer))
                                  (::sys/args (meta #'->ingest-only-tx-log)))}
  [opts]
  (let [tx-log (->ingest-only-tx-log opts)]
    (-> tx-log
        (assoc :tx-consumer (tx/->polling-tx-consumer
                              opts
                              (fn [after-tx-id]
                                (db/open-tx-log tx-log after-tx-id)))))))

(defrecord JedisDocumentStore [^JedisPool jedis-pool prefix]
  db/DocumentStore
  (submit-docs [this docs]
    (when (seq docs)
      (with-open [^Jedis j (.getResource jedis-pool)]
        (.mset j
               ^"[[B"
               (into-array
                 (mapcat
                   (fn [[id doc]]
                     [(.getBytes (str prefix id) "UTF-8")
                      (nippy/freeze doc)])
                   docs)))))
    nil)

  (fetch-docs [this ids]
    (if (seq ids)
      (zipmap ids
              (map nippy/thaw
                   (with-open [^Jedis j (.getResource jedis-pool)]
                     (.mget j ^"[[B" (into-array
                                       (map #(.getBytes (str prefix %) "UTF-8")
                                            ids))))))
      {})))

(defn ->document-store {::sys/args {:prefix {:required? true,
                                             :spec ::sys/string
                                             :doc "Prefix to use on keys, e.g. \"crux-document:\""
                                             :default "crux-docs:"}
                                    :doc-cache-size ds/doc-cache-size-opt}
                        ::sys/deps {:jedis-pool `->jedis-pool}}
  [{:keys [prefix jedis-pool doc-cache-size]}]
  (ds/->CachedDocumentStore
    (lru/new-cache doc-cache-size)
    (->JedisDocumentStore jedis-pool prefix)))

(comment
  (require '[crux.api :as crux])
  (def node (crux/start-node
              {:jedis-pool {:crux/module `->jedis-pool
                            :host "localhost"}
               :crux/tx-log {:crux/module `->tx-log
                             :jedis-pool :jedis-pool
                             :poll-sleep-consumer (java.time.Duration/ofSeconds 10)}
               :crux/document-store {:crux/module `->document-store
                                     :jedis-pool :jedis-pool}}))
  (.close node)

  (crux/submit-tx node [[:crux.tx/put {:crux.db/id :ivan, :age 40}]])
  (crux/submit-tx node [[:crux.tx/put {:crux.db/id :joe, :age 100}]])
  (crux/submit-tx node [[:crux.tx/put {:crux.db/id 1 :age 6 :friend :joe}]])
  (dotimes [n 30000]
    (crux/submit-tx node [[:crux.tx/put {:crux.db/id (+ n (* 1 1000))
                                         :age (rand-int 50)
                                         :friend (rand-nth [:ivan :joe :sven])}]]))
  (crux/entity (crux/db node) :joe)
  (crux/q (crux/db node)
          '{:find [?e ?a ?f]
            :where [[?e :age ?a]
                    [?e :friend ?f]]}))
