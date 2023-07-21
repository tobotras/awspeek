(ns awspeek.core
  (:require [amazonica.aws.s3 :as s3]
            [amazonica.aws.ec2 :as ec2]
            [amazonica.aws.eks :as eks]
            [kubernetes-api.core :as k8s]
            [clojure.pprint :as pp]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.java.shell :as shell]
            [clojure.set :as set]
            [yaml.core :as yaml]
            [next.jdbc :as jdbc]
            [next.jdbc.prepare :as prep]
            [next.jdbc.result-set :as rs]
            [honey.sql :as sql]
            [clojure-ini.core :as ini])
  (:gen-class))

(def max-object-to-dump 1024) ;1GB

;; DB

(def regexps [])
(def db-opts (atom nil))
(def log-statement (atom nil))

(defn sql! [request]
  (jdbc/execute! @db-opts (sql/format request) {:builder-fn rs/as-unqualified-lower-maps}))

(defn load-regexps []
  ;; SELECT REGEXPS.LABEL, REGEXPS.REGEX, DATA_CLASSES.NAME FROM REGEXPS INNER JOIN DATA_CLASSES ON REGEXPS.CLASS = DATA_CLASS.ID;
  (let [rs (sql! {:select [:regexps.id :regexps.label :regexps.regex [:data_classes.name :class]]
                  :from [:regexps]
                  :right-join [:data_classes [:= :regexps.class :data_classes.id]]})]
    (alter-var-root (var regexps)
                    (fn [_]
                      (mapv #(let [re-string (:regex %)
                                   ;; Precompile regexps, removing "^...$" if found
                                   re (.substring (java.lang.String. re-string)
                                                  (if (= (first re-string) \^)
                                                    1
                                                    0)
                                                  (- (count re-string)
                                                     (if (= (first (take-last 1 re-string)) \$)
                                                       1
                                                       0)))]
                               (assoc % :pattern (re-pattern re)))
                            rs)))))

(defn with-db [cfg body]
  (with-open [con (jdbc/get-connection cfg)]
    (let [opts (jdbc/with-options con {:auto-commit false})]
      (jdbc/with-transaction [tx opts]
        (swap! db-opts (fn [_] tx))
        (load-regexps)
        (body)))))

(def profile-region
  (memoize #(let [profile-name (System/getenv "AWS_PROFILE")
                  cfg (ini/read-ini (io/file (System/getenv "HOME") ".aws" "config"))]
              (get-in cfg [(str "profile " profile-name) "region"]))))

(defn mark-match [asset resource folder object re-id]
  (when (nil? @log-statement)
    (swap! log-statement
           (fn [_]
             (jdbc/prepare @db-opts ["insert into matches (asset, resource, location, folder, file, regexp)
                                      values((select id from assets where name=?),?,?,?,?,?)"]))))
  (prep/set-parameters @log-statement [asset resource (profile-region) folder object re-id])
  (.addBatch @log-statement))

(defn grep-line [asset resource folder file line]
  (doseq [re regexps]
    (when (re-find (:pattern re) line)
      (mark-match asset resource folder file (:id re)))))

(defn gzipped-stream? [s]
  (let [header (byte-array 2)]
    (.mark s 2)
    (.read s header)
    (.reset s)
    (and (= (first header) 31)
         (= (second header) -117))))

(defn grep-stream [stream bucket object-name]
  (let [lines (line-seq (io/reader stream))]
    (doseq [[i line] (map-indexed vector lines)]
      (if (zero? (mod i 300000))
        (println i (java.util.Date.)))
      (grep-line "AWS" "S3" bucket object-name line)))
  (when-not (nil? @log-statement)
    (.executeBatch @log-statement)
    (swap! log-statement (fn [_] nil))))

(defn grep-object [bucket object-name]
  (let [raw-stream (io/input-stream (:input-stream (s3/get-object bucket object-name)))
        input-stream (if (gzipped-stream? raw-stream)
                       (java.util.zip.GZIPInputStream. raw-stream)
                       raw-stream)]
    (grep-stream input-stream bucket object-name)))

(defn process-bucket [bucket]
  (println "Bucket:" bucket)
  ;; TODO: pagination?
  (let [object-list (s3/list-objects {:bucket-name bucket})
        objects (second (first (filter #(= (first %) :object-summaries) object-list)))]
    (doseq [obj objects :when (<= (:size obj) (* max-object-to-dump 1024 1024))]
      (println "Object key:" (:key obj) ", size" (:size obj))
      (grep-object bucket (:key obj)))))

(defn process-s3 []
  ;;DEBUG: local file
  ;;(let [s (io/input-stream "/tmp/xtalk.mail.tobotras")]
  ;; (grep-stream s "no-bucket" "tmpfile"))
  (let [bucket-list (s3/list-buckets)]
    (doseq [bucket bucket-list]
      (process-bucket (:name bucket)))))

;; ----------
(defn tools-env [var & [default]]
  (if-let [value (System/getenv var)]
    (if (number? default)
      (Integer/parseUnsignedInt value)
      value)
    default))
;;-------------

;; Assumption: AWS env vars (AWS_PROFILE)
(defn -main [& args]
  (println "region:" (profile-region))
  (with-db {:dbtype "postgresql"
            :dbname   (tools-env "DB_NAME" "ximi")
            :host     (tools-env "DB_HOST" "localhost")
            :user     (tools-env "DB_USER" "ximi")
            :password (tools-env "DB_PASS" "ximipass")}
    process-s3)
  (System/exit 0))
