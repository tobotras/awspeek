(ns group.ximi.awspeek.core
  (:require [amazonica.aws.s3 :as s3]
            [amazonica.aws.rds :as rds]
            [clojure.pprint :as pp]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [next.jdbc :as jdbc]
            [next.jdbc.prepare :as prep]
            [next.jdbc.result-set :as rs]
            [honey.sql :as sql]
            [clojure-ini.core :as ini]
            [com.climate.claypoole :as cp])
  (:gen-class))

;; ----------
(defn tools-env [var & [default]]
  (if-let [value (System/getenv var)]
    (if (number? default)
      (Integer/parseUnsignedInt value)
      value)
    default))
;;-------------

(def max-object-size (* 1024 1024 1024)) ;1GB
(def data-store {:dbtype "postgresql"
                 :dbname   (tools-env "DB_NAME" "ximi")
                 :host     (tools-env "DB_HOST" "localhost")
                 :user     (tools-env "DB_USER" "ximi")
                 :password (tools-env "DB_PASS" "ximipass")})

;; FIXME: globals

(def regexps [])
(def db-conn (atom nil))
(def match-stmt (atom nil))
(def t-pool (atom nil))

(defn sql! [request]
  (jdbc/execute! @db-conn (sql/format request) {:builder-fn rs/as-unqualified-lower-maps}))

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
                               (assoc % :pattern (re-pattern (clojure.string/escape re {\\ "\\"}))))
                            rs)))))

;; Assumption: UNIX env
(def profile-region
  (memoize #(-> (System/getenv "HOME")
                (io/file ".aws" "config")
                ini/read-ini
                (get-in [(str "profile " (System/getenv "AWS_PROFILE")) "region"]))))

(defn mark-match [asset resource location folder object re-id]
  (when (nil? @match-stmt)
    (swap! match-stmt
           (fn [stmt]
             (when (nil? stmt)
               (jdbc/prepare @db-conn ["insert into matches (asset, resource, location, folder, file, regexp)
                                      values((select id from assets where name=?),?,?,?,?,?)"])))))
  (prep/set-parameters @match-stmt [asset resource location folder object re-id])
  (.addBatch @match-stmt))

(defn grep-line [asset resource location folder file line]
  (when (nil? @t-pool)
    (swap! t-pool (fn [_] (cp/threadpool 4))))
  ;;FIXME: stops after some 2mln lines :-O
  ;;(cp/upfor t-pool [re regexps]
  (doseq [re regexps]
    (when (re-find (:pattern re) line)
      (mark-match asset resource location folder file (:id re)))))

(defn gzipped-stream? [s]
  (let [header (byte-array 2)]
    (.mark s 2)
    (.read s header)
    (.reset s)
    (and (= (first header) 31)
         (= (second header) -117))))

(defn commit-batch! []
  (when-not (nil? @match-stmt)
    (.executeBatch @match-stmt)
    (swap! match-stmt (fn [_] nil)))
  (.commit @db-conn))

(defn grep-stream [s asset resource location folder object]
  (let [stream (if (gzipped-stream? s)
                       (java.util.zip.GZIPInputStream. s)
                       s)]
    (let [lines (line-seq (io/reader stream))]
      (doseq [[i line] (map-indexed vector lines)]
        (if (zero? (mod i 300000))
          (println i (java.util.Date.)))
        (grep-line asset resource location folder object line)))
    (commit-batch!)))

(defn grep-s3-object [bucket object-name]
  (let [stream (io/input-stream (:input-stream (s3/get-object bucket object-name)))]
    (grep-stream stream "AWS" "S3" (profile-region) bucket object-name)))

(defn process-s3-bucket [bucket]
  (println "Bucket:" bucket)
  ;; TODO: pagination?
  (let [objects (->> (s3/list-objects {:bucket-name bucket})                   
                     (filter #(= (first %) :object-summaries))
                     first
                     second)]
    ;;objects (second (first (filter #(= (first %) :object-summaries) object-list)))]
    (doseq [obj objects :when (<= (:size obj) max-object-size)]
      (println "Object key:" (:key obj) ", size" (:size obj))
      (grep-s3-object bucket (:key obj)))))

(defn aws-creds? []
  (if (System/getenv "AWS_PROFILE")
    true
    (do
      (println "AWS_PROFILE not set, skipping S3 processing")
      false)))

(defn process-s3 []
  (when (aws-creds?)
    ;; TODO: pagination?
    (doseq [bucket (s3/list-buckets)]
      (process-s3-bucket (:name bucket)))))

;;--------------------------------------------------------

(defn hostname []
  (-> "hostname"
      shell/sh
      :out
      clojure.string/trim-newline))

(defn process-local-file [file-name]
  (let [file (io/file file-name)
        dirName (-> file
                    .getAbsoluteFile
                    .getParent)]
    (-> file-name
        io/input-stream
        (grep-stream "Filesystem" "Local file" (hostname) dirName (.getName file)))))

(defn text-column? [col]
  ;; FIXME: datatype IDs are PostgreSQL specific, I guess!
  (let [text-data-types ["12"]]
    (some #(= (:type col) %) text-data-types)))

(defn process-row [asset resource location folder file row]
  (doseq [key (keys row)]
    (grep-line asset resource location folder file (get row key))))

(defn process-columns [asset resource location folder conn cols table-name]
  (let [text-cols (filter text-column? cols)]
    (if (empty? text-cols)
      (println table-name "has no text columns")
      (let [cols-list (mapv #(keyword (:name %)) text-cols)
            rs (jdbc/execute! conn (sql/format {:select cols-list
                                                :from (keyword table-name)
                                                :limit 1}) {:builder-fn rs/as-unqualified-lower-maps})]
        (doseq [row rs]
          (process-row asset resource location folder table-name row))))))

(defn process-table [asset resource location folder conn metadata table-name]
  (println "* * Processing table" table-name)
  (let [rs (.getColumns metadata nil nil table-name nil)]
    (loop [cols []]
      (if (.next rs)
        (recur (conj cols {:name (.getString rs "COLUMN_NAME")
                           :type (.getString rs "DATA_TYPE")}))
        (process-columns asset resource location folder conn cols table-name)))))

(defn process-tables [asset resource location folder conn metadata tables]
  (if (empty? tables)
    (println "* * No tables")
    (doseq [table-name tables]
      (process-table asset resource location folder conn metadata table-name)
      (commit-batch!))))

(defn process-psql [asset datasource]
  (if-let [conn (jdbc/get-connection datasource)]
    (let [metadata (.getMetaData conn)
          rs (.getTables metadata nil nil nil (into-array ["TABLE"]))]
      (loop [tables []]
        (if (.next rs)
          (recur (conj tables (.getString rs "TABLE_NAME")))
          (process-tables asset (:dbtype datasource) (:host datasource) (:dbname datasource) conn metadata tables))))
    (println "Can't connect to DB")))

;;-----------------------------------------------------

(defn discover-db [datasource]
  (with-open [conn (jdbc/get-connection datasource)]
    (let [rs (jdbc/execute! conn
                            ["SELECT datname FROM pg_database WHERE datistemplate = false AND datname <> 'postgres' AND datname <> 'rdsadmin'"]
                            {:builder-fn rs/as-unqualified-lower-maps})]
      (map #(:datname %) rs))))

(defn process-rds-postgres [instance]
  (let [status (:dbinstance-status instance)]
    (if (= status "available")
      (let [endpoint (:endpoint instance)
            datasource {:dbtype "postgresql"
                        :user (:master-username instance)
                        :host (:address endpoint)
                        :port (:port endpoint)
                        :password "qwe123qwe123"} ;;FIXME!!
            db-list (discover-db datasource)]
        (if (empty? db-list)
          (println "* No databases in the instance")
          (doseq [db-name db-list]
            (println "* Processing database" db-name)
            (process-psql "AWS" (assoc datasource :dbname db-name)))))
      (println (format "* Instance status: '%s', skipping" status)))))

(defn process-rds-instance [instance]
  (println "RDS instance:" (:dbinstance-identifier instance))
  (if (= (:engine instance) "postgres")
    (process-rds-postgres instance)
    (println (format "Unsupported engine: '%s'" (:engine instance)))))

(defn process-rds []
  (when (aws-creds?)
    (doseq [instance (:dbinstances (rds/describe-db-instances))]
      (process-rds-instance instance))))
;;-----------------------------------------------------

(defn usage []
  (println "Usage:
awspeek --aws-s3
   or
awspeek --aws-rds
   or
awspeek --local FILENAME
   or
awspeek --psql HOST PORT USER PASS DB")
  (System/exit 1))

(defn -main [& args]
  (when (empty? args)
    (usage))
  (swap! db-conn (fn [_]
                   (jdbc/get-connection data-store
                                        {:auto-commit false
                                         :reWriteBatchedInserts true})))
  (load-regexps)
  (case (first args)
    "--aws-s3"    (process-s3)
    "--aws-rds"   (process-rds)
    "--local"     (if (= (count args) 2)
                    (process-local-file (second args))
                    (usage))
    "--psql"      (if (= (count args) 5)
                    (let [datasource {:dbtype "postgresql"
                                      :host (nth args 1)
                                      :port (nth args 2)
                                      :user (nth args 3)
                                      :password (nth args 4)
                                      :dbname (nth args 5)}]
                      (process-psql "Onprem" datasource))
                    (usage))
    (usage))
  (when @t-pool
    (cp/shutdown @t-pool))
  (System/exit 0))
