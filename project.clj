(defproject awspeek "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [amazonica "0.3.165"]
                 [nubank/k8s-api "0.1.2"]
                 [io.forward/yaml "1.0.11"]
                 [org.postgresql/postgresql "42.2.10"]
                 [com.github.seancorfield/next.jdbc "1.3.834"]
                 [com.github.seancorfield/honeysql "2.3.928"]
                 [org.clojure/data.json "2.4.0"]]
  :main ^:skip-aot awspeek.core
  :target-path "target/%s"
  :jvm-opts []
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
