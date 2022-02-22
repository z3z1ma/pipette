(defproject pipette "0.4.5"

  :description "Engine for processing new line delimited json emitted to stdout and sent via a pipe"

  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/data.json "2.4.0"]
                 [org.clojure/java.jdbc "0.7.12"]
                 [org.clojure/tools.cli "1.0.206"]
                 [org.postgresql/postgresql "42.3.1"]
                 [com.github.seancorfield/next.jdbc "1.2.761"]
                 [com.github.seancorfield/honeysql "2.2.840"]
                 [io.forward/yaml "1.0.11"]
                 [metosin/jsonista "0.3.5"]]

  :main pipette.core
  :aot [pipette.core]

  :uberjar-name "pipette.jar"

  :repl-options {:init-ns pipette.core}
  :profiles {:dev {:uberjar {:aot :all
                             :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}
                   :plugins [[lein-shell "0.5.0"]
                             [autodoc/lein-autodoc "1.1.1"]
                             [autodoc "1.1.1"]]}}

  :aliases {"native" ["shell"
                      "native-image"
                      "-jar" "./target/pipette.jar}"
                      "-H:Name=./target/pipette"]})
