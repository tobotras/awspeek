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
            [com.climate.claypoole :as cp]
            ;;[clj-async-profiler.core :as prof]
            [clojure.tools.cli :as cli]
            [clojure.string :as str])
  (:gen-class))

;; Tools
(defn tools-env [var & [default]]
  (if-let [value (System/getenv var)]
    (if (number? default)
      (Integer/parseUnsignedInt value)
      value)
    default))

;; Configuration
(def max-object-size (* 1024 1024 1024)) ;1GB
(def data-store {:dbtype "postgresql"
                 :dbname   (tools-env "DB_NAME" "ximi")
                 :host     (tools-env "DB_HOST" "localhost")
                 :user     (tools-env "DB_USER" "ximi")
                 :password (tools-env "DB_PASS" "ximipass")})

;;-------------

;; Wait, what are those, GLOBALS?!
(def regexps [])
(def db-conn (atom nil))
(def match-stmt (atom nil))
(def match-count (atom {}))
(def t-pool (atom nil))
(def cli-opts (atom {}))

(defn precompile-regex
  "Precompile regexps, removing '^...$' if found"
  [re-spec]
  (let [re-string (:regex re-spec)
        re (.substring (java.lang.String. ^java.lang.String re-string)
                       (if (= (first re-string) \^)
                         1
                        0)
                       (- (count re-string)
                          (if (= (first (take-last 1 re-string)) \$)
                            1
                            0)))]
    (assoc re-spec :pattern (re-pattern (str/escape re {\\ "\\"})))))
  
(def jdbc-execute! [conn request]
  (jdbc/execute! conn (sql/format request) {:builder-fn rs/as-unqualified-lower-maps}))

(defn load-regexps []
  ;; SELECT REGEXPS.LABEL, REGEXPS.REGEX, DATA_CLASSES.NAME FROM REGEXPS INNER JOIN DATA_CLASSES ON REGEXPS.CLASS = DATA_CLASS.ID;
  (let [rs (jdbc-execute! @db-conn {:select [:regexps.id :regexps.label :regexps.regex [:data_classes.name :class]]
                                    :from [:regexps]
                                    :right-join [:data_classes [:= :regexps.class :data_classes.id]]})]
    (alter-var-root (var regexps) (fn [_] (mapv precompile-regex rs)))))

;; Assumption: UNIX env
(def profile-region
  (memoize #(-> (System/getenv "HOME")
                (io/file ".aws" "config")
                ini/read-ini
                (get-in [(str "profile " (System/getenv "AWS_PROFILE")) "region"]))))

(defn obj-path [asset resource location folder object]
  (clojure.string/join "/" [asset resource location folder object]))

;; Returns nil if too many matches already, true to continue
(defn mark-match [asset resource location folder object re-id]
  (when (nil? @match-stmt)
    (swap! match-stmt
           (fn [_]
             (jdbc/prepare @db-conn ["insert into matches (asset, resource, location, folder, file, regexp)
                                      values((select id from assets where name=?),?,?,?,?,?)"]))))
  (prep/set-parameters @match-stmt [asset resource location folder object re-id])
  (.addBatch ^java.sql.PreparedStatement @match-stmt)
  (let [k (obj-path asset resource location folder object)
        count (inc (get @match-count k 0))]
    (swap! match-count #(update % k (constantly count)))
    (if-let [maxmatches (:maxmatches @cli-opts)]
      (if (>= count maxmatches)
        (do
          (when (:verbosity @cli-opts)
            (println "Too many matches for" k))
          nil)
        true)
      true)))

;; Returns nil if too many matches already, true to continue
(defn grep-line [asset resource location folder file line]
  (when (nil? @t-pool)
    (swap! t-pool (constantly (cp/threadpool 4))))
  ;;FIXME: stops after some 2mln lines :-O
  ;;(cp/upfor t-pool [re regexps]
  (loop [regexps-todo regexps]
    (if-let [re (first regexps-todo)]
      (if (re-find (:pattern re) line)
        (when (mark-match asset resource location folder file (:id re))
          (recur (rest regexps-todo)))
        (recur (rest regexps-todo)))
      true)))

(defn gzipped-stream? [^java.io.BufferedInputStream stream]
  (let [header (byte-array 2)]
    (doto stream
      (.mark 2)
      (.read header)
      (.reset))
    (if (and (= (first header) 31)
             (= (second header) -117))
      (do
        (when (:verbosity @cli-opts)
          (println "Gzipped stream found"))
        true)
      false)))

(defn commit-batch! []
  (when @match-stmt
    (.executeBatch ^org.postgresql.jdbc.PgPreparedStatement @match-stmt)
    (swap! match-stmt (constantly nil)))
  (.commit ^org.postgresql.jdbc.PgConnection @db-conn))

(defn grep-stream [s asset resource location folder object]
  (when (:verbosity @cli-opts)
    (println "Processing" (obj-path asset resource location folder object)))
  (let [stream (if (gzipped-stream? s)
                 (java.util.zip.GZIPInputStream. s)
                 s)
        lines (line-seq (io/reader stream))]
    (loop [i-lines (map-indexed vector lines)]
      (let [[i line] (first i-lines)]
        (when-not (nil? line)
          (when (and (zero? (mod i 300000))
                     (:verbosity @cli-opts))
            (println i (java.util.Date.)))
          (when (grep-line asset resource location folder object line)
            (recur (rest i-lines))))))
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

(defn check-aws-creds []
  (when-not (System/getenv "AWS_PROFILE")
    (println "AWS_PROFILE not set, cannot access AWS")
    (System/exit 2)))

(defn process-s3 []
  (check-aws-creds)
  ;; TODO: pagination?
  (doseq [bucket (s3/list-buckets
                  ;;{:endpoint "https://storage.api.il.nebius.cloud/"}
                  )]
    (process-s3-bucket (:name bucket))))

;;--------------------------------------------------------

(defn hostname []
  (-> "hostname"
      shell/sh
      :out
      str/trim-newline))

(defn process-local-file [file-name]
  (println "Processing" file-name)
  (let [file (io/file file-name)
        dirName (-> file
                    .getAbsoluteFile
                    .getParent)]
    (-> file
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
            rs (jdbc-execute! conn {:select cols-list
                                    :from (keyword table-name)
                                    :limit 1})]
        (doseq [row rs]
          (process-row asset resource location folder table-name row))))))

(defn process-psql-table [asset resource location folder conn
                          ^org.postgresql.jdbc.PgDatabaseMetaData metadata table-name]
  (println "* * Processing table" table-name)
  (let [rs (.getColumns metadata nil nil table-name nil)]
    (loop [cols []]
      (if (.next rs)
        (recur (conj cols {:name (.getString rs "COLUMN_NAME")
                           :type (.getString rs "DATA_TYPE")}))
        (process-columns asset resource location folder conn cols table-name)))))

(defn process-psql-tables [asset resource location folder conn metadata tables]
  (if (empty? tables)
    (println "* * No tables")
    (doseq [table-name tables]
      (process-psql-table asset resource location folder conn metadata table-name)
      (commit-batch!))))

(defn process-psql [asset datasource]
  (if-let [conn (jdbc/get-connection datasource)]
    (let [metadata (.getMetaData conn)
          rs (.getTables metadata nil nil nil (into-array ["TABLE"]))]
      (loop [tables []]
        (if (.next rs)
          (recur (conj tables (.getString rs "TABLE_NAME")))
          (process-psql-tables asset (:dbtype datasource) (:host datasource) (:dbname datasource) conn metadata tables))))
    (println "Can't connect to DB")))

;;-----------------------------------------------------

(defn discover-db [datasource]
  (with-open [conn (jdbc/get-connection datasource)]
    (map :datname
         (jdbc-execute! conn {:select :datname
                              :from [:pg_database]
                              :where [:and
                                      [:= :datistemplate false]
                                      [:not= :datname "postgres"]
                                      [:not= :datname "rdsadmin"]]}))))

(defn process-rds-postgres [instance]
  (let [status (:dbinstance-status instance)]
    (if (= status "available")
      (let [endpoint (:endpoint instance)
            datasource {:dbtype "postgresql"
                        :user (:master-username instance)
                        :host (:address endpoint)
                        :port (:port endpoint)
                        :password "qwe123QWE123"} ;;FIXME!!
            _ (println "Connecting to" endpoint)
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
  (check-aws-creds)
  (doseq [instance (:dbinstances (rds/describe-db-instances))]
    (process-rds-instance instance)))
;;-----------------------------------------------------

(defn usage [parsed]
  (println "Usage:")
  (println (:summary parsed))
  (System/exit 1))

(defn do-work [parsed]
  (with-open [c (jdbc/get-connection data-store
                                     {:auto-commit false
                                      :reWriteBatchedInserts true})]
    (swap! db-conn (constantly c))
    (load-regexps)
    (cond
      (:aws-s3 @cli-opts) (process-s3)
      (:aws-rds @cli-opts) (process-rds)
      (:file @cli-opts) (process-local-file (:file @cli-opts))
      (:dbname @cli-opts) (let [datasource {:dbtype "postgresql"
                                            :host (:host @cli-opts)
                                            :port (:port @cli-opts)
                                            :user (:user @cli-opts)
                                            :password (:password @cli-opts)
                                            :dbname (:dbname @cli-opts)}]
                            (if (some nil? (vals datasource))
                              (usage parsed)
                              (process-psql "Omprem" datasource)))
      :else (usage parsed)))
  (let [total-matches (reduce + (vals @match-count))]
    (when (pos? total-matches)
      (println "Matches registered:" total-matches))))

(defn -main [& args]
  (let [parsed (cli/parse-opts args
                               [["-v" "--verbose" "Verbosity"
                                 :id :verbosity
                                 :default 0
                                 :update-fn inc]
                                ["-s" "--aws-s3" "Process AWS S3 storage"]
                                ["-r" "--aws-rds" "Process AWS RDS tables"]
                                ["-f" "--file FILE" "Proces local text file"
                                 :validate [#(.exists (io/file %)) "No such file"]]
                                ["-d" "--dbname DATABASE" "PostgreSQL database name"]
                                ["-H" "--host HOSTNAME" "PostgreSQL server host"]
                                ["-p" "--port PORT" "PostgreSQL server port"
                                 :default 5432
                                 :parse-fn #(Integer/parseInt %)
                                 :validate [#(< 0 % 65536) "Invalid port number"]]
                                ["-u" "--user USERNAME" "PostgreSQL server username"]
                                ["-w" "--password PASS" "PostgreSQL server password"]
                                ["-m" "--maxmatches N" "Process up to N matches per source"
                                 :parse-fn #(Integer/parseInt %)
                                 :validate [#(> % 0) "Invalid number of matches"]
                                 ]
                                ["-h" "--help" "Show this help"]])
        err (:errors parsed)]
    (when err
      (println (first err))
      (System/exit 0))
    (swap! cli-opts (constantly (:options parsed)))
    (if (:help @cli-opts)
      (usage parsed)
      (do-work parsed)))
  (System/exit 0))                      ; or else

