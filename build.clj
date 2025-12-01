(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'mud)
(def version "0.1.0")
(def class-dir "target/classes")
(def basis (b/create-basis {}))
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn clean [_]
  (b/delete {:path "target"})
  (println "Cleaned target"))

;; (defn jar [_]
;;   (clean nil)
;;   (b/copy-dir {:src-dirs ["src" "resources"]
;;                :target-dir class-dir})
;;   (b/jar {:class-dir class-dir
;;           :jar-file jar-file
;;           :basis basis})
;;   (println "Created jar:" jar-file))


(defn uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir class-dir})

  (b/uber {:class-dir class-dir
           :uber-file jar-file
           :basis basis
           :main 'clojure.main})

  (println "Created jar:" jar-file))
