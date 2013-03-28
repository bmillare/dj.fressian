;; Copyright (c) Metadata Partners, LLC.
;; All rights reserved.

(ns dj.fressian
  (:refer-clojure :exclude [pr read])
  (:require [clojure.java.io :as io])
  (:import
   [java.io InputStream OutputStream EOFException]
   java.nio.ByteBuffer
   java.nio.charset.Charset
   [org.fressian FressianWriter StreamingWriter FressianReader Writer Reader]
   [org.fressian.handlers WriteHandler ReadHandler ILookup  WriteHandlerLookup]
   [org.fressian.impl ByteBufferInputStream BytesOutputStream]))

;; move into get, a la Clojure lookup?
(defn as-lookup
  "Normalize ILookup or map into an ILookup."
  [o]
  (if (map? o)
    (reify ILookup
           (valAt [_ k] (or (get o k)
                            (get o (.getSuperclass k)))))
    o))

(defn write-handler-lookup
  "Returns a fressian write handler lookup that combines fressian's built-in
   handlers with custom-lookup. custom-lookup can be a map or an ILookup,
   keyed by class and returning a single-entry map of tag->write handler.
   Use this to create custom validators, not to create FressianWriters, as
   the latter already call customWriteHandlers internally."
  [custom-lookup]
  (WriteHandlerLookup/createLookupChain (as-lookup custom-lookup)))

(defn ^Writer create-writer
  "Create a fressian writer targetting out. lookup can be an ILookup or
   a nested map of type => tag => WriteHandler."
  ;; TODO: make symmetric with create-reader, using io/output-stream?
  ([out] (create-writer out nil))
  ([out lookup]
     (FressianWriter. out (as-lookup lookup))))

(defn ^Reader create-reader
  "Create a fressian reader targetting in, which must be compatible
   with clojure.java.io/input-stream.  lookup can be an ILookup or
   a map of tag => ReadHandler."
  ([in] (create-reader in nil))
  ([in lookup] (create-reader in lookup true))
  ([in lookup validate-checksum]
     (FressianReader. (io/input-stream in) (as-lookup lookup) validate-checksum)))

(defn fressian
  "Fressian obj to output-stream compatible out.

   Options:
      :handlers    fressian handler lookup
      :footer      true to write footer"
  [out obj & {:keys [handlers footer]}]
  (with-open [os (io/output-stream out)]
    (let [writer (create-writer os handlers)]
      (.writeObject writer obj)
      (when footer
        (.writeFooter writer)))))

(defn defressian
  "Read single fressian object from input-stream-compatible in.

   Options:
      :handlers    fressian handler lookup
      :footer      true to validate footer"
  ([in & {:keys [handlers footer]}]
     (let [fin (create-reader in handlers)
           result (.readObject fin)]
       (when footer (.validateFooter fin))
       result)))

(defn ^ByteBuffer bytestream->buf
  "Return a readable buf over the current internal state of a
   BytesOutputStream."
  [^BytesOutputStream stream]
  (ByteBuffer/wrap (.internalBuffer stream) 0 (.length stream)))

(defn byte-buffer-seq
  "Return a lazy seq over the remaining bytes in the buffer.
   Not fast: intented for REPL usage.
   Works with its own duplicate of the buffer."
  [^ByteBuffer bb]
  (lazy-seq
   (when (.hasRemaining bb)
     (let [next-slice (.slice bb)]
       (cons (.get next-slice) (byte-buffer-seq next-slice))))))

(defn ^ByteBuffer byte-buf
  "Return a byte buffer with the fressianed form of object.
   See fressian for options."
  [obj & options]
  (let [baos (BytesOutputStream.)]
    (apply fressian baos obj options)
    (bytestream->buf baos)))

(defn read-batch
  "Read a fressian reader fully (until eof), returning a (possibly empty)
   vector of results."
  [^Reader fin]
  (let [sentinel (Object.)]
    (loop [objects []]
      (let [obj (try (.readObject fin) (catch EOFException e sentinel))]
        (if (= obj sentinel)
          objects
          (recur (conj objects obj)))))))

(defmacro ->write-tag-handler
  "args must be [writer data-object]"
  [tag size args & body]
  `(let [tag# ~tag]
     {tag#
      (reify WriteHandler (~'write ~(into [(gensym)] args)
                            (.writeTag ~(first args) tag# ~size)
                            ~@body))}))

(def clojure-write-handlers
  {clojure.lang.Keyword
   (->write-tag-handler "key" 2
                        [w s]
                        (.writeObject w (namespace s))
                        (.writeObject w (name s)))
   clojure.lang.Symbol
   (->write-tag-handler "sym" 2
                        [w s]
                        (.writeObject w (namespace s))
                        (.writeObject w (name s)))
   ;; Note, you cannot override core handlers, if we want to read a
   ;; list as a vector you must tag it first when written then make a
   ;; custom reader for that tag type
   clojure.lang.PersistentVector
   (->write-tag-handler "vec" 1
                        [w s]
                        ;; Note that when delegating writing,
                        ;; .writeObject dispatches based on existing
                        ;; handlers, its easy to get a SO if you
                        ;; keep delegating to yourself. Therefore
                        ;; you must be narrowing down the handler in
                        ;; some way.
                        (.writeList w s))})

(defmacro ->read-tag-handler
  "args must be [reader data-object]"
  [[reader] & body]
  `(reify ReadHandler (~'read ~[(gensym) reader (gensym) (gensym)]
                        ~@body)))

(def clojure-read-handlers
  {"key"
   (->read-tag-handler [rdr]
                       (keyword (.readObject rdr) (.readObject rdr)))
   "sym"
   (->read-tag-handler [rdr]
                       (symbol (.readObject rdr) (.readObject rdr)))
   "map"
   (->read-tag-handler [rdr]
                       (let [kvs ^java.util.List (.readObject rdr)]
                         (if (< (.size kvs) 16)
                           (clojure.lang.PersistentArrayMap. (.toArray kvs))
                           (clojure.lang.PersistentHashMap/create (seq kvs)))))
   "vec"
   (->read-tag-handler [rdr]
                       (vec (.readObject rdr)))})

(extend ByteBuffer
  io/IOFactory
  (assoc io/default-streams-impl
    :make-input-stream (fn [x opts] (io/make-input-stream
                                     (ByteBufferInputStream. x) opts))))

(defn ->poop-fn
  "specify your own default handlers"
  [handlers]
  (fn [out obj]
    (with-open [os (io/output-stream out)]
      (let [writer (create-writer os handlers)]
        (.writeObject writer obj)))))

(defn ->eat-fn
  "specify your own default handlers"
  [handlers]
  (fn [in]
    (let [fin (create-reader in handlers)
          result (.readObject fin)]
      result)))

(def poop
  "
Convenience wrapper over fressian, provides default handlers

out: any output endpoint such as a file

Supports any endpoint that clojure.java.io/output-stream can handle

obj: your object to emit
"
  (->poop-fn clojure-write-handlers))

(def eat
  "
Convenience wrapper over deffressian, provides default handlers

in: any input starting point such as a file

Supports any starting point that clojure.java.io/input-stream can handle

returns the object
"
  (->eat-fn clojure-read-handlers))

