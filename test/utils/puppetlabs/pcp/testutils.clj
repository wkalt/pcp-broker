(ns puppetlabs.pcp.testutils
  (:require [clojure.test :refer :all]
            [puppetlabs.trapperkeeper.testutils.bootstrap :refer [with-app-with-config]]
            [puppetlabs.pcp.broker.service :refer [broker-service]]
            [puppetlabs.trapperkeeper.services.authorization.authorization-service :refer [authorization-service]]
            [puppetlabs.trapperkeeper.services.metrics.metrics-service :refer [metrics-service]]
            [puppetlabs.trapperkeeper.services.status.status-service :refer [status-service]]
            [puppetlabs.trapperkeeper.services.webrouting.webrouting-service :refer [webrouting-service]]
            [puppetlabs.trapperkeeper.services.webserver.jetty9-service :refer [jetty9-service]]
            [puppetlabs.trapperkeeper.app :as tka]
            ))

(def ^:dynamic *broker*)

(defmacro dotestseq [bindings & body]
  (if-not (seq bindings)
    `(do ~@body)
    (let [case-versions (remove keyword? (take-nth 2 bindings))]
      `(doseq ~bindings
         (testing (str "with " (clojure.string/join
                                ", "
                                (map #(str (pr-str %1) ": " (pr-str %2))
                                     '~case-versions
                                     (list ~@case-versions))))
           ~@body)))))

(def broker-services
  "The trapperkeeper services the broker needs"
  [authorization-service broker-service jetty9-service webrouting-service metrics-service status-service])

(def broker-config
  "A broker with ssl and own spool"
  {:authorization {:version 1
                   :rules [{:name "allow all"
                            :match-request {:type "regex"
                                            :path "^/.*$"}
                            :allow-unauthenticated true
                            :sort-order 1}]}

   :webserver {:ssl-host "127.0.0.1"
               ;; usual port is 8142.  Here we use 8143 so if we're developing
               ;; we can run a long-running instance and this one for the
               ;; tests.
               :ssl-port 8143
               :client-auth "want"
               :ssl-key "./test-resources/ssl/private_keys/broker.example.com.pem"
               :ssl-cert "./test-resources/ssl/certs/broker.example.com.pem"
               :ssl-ca-cert "./test-resources/ssl/ca/ca_crt.pem"
               :ssl-crl-path "./test-resources/ssl/ca/ca_crl.pem"}

   :web-router-service
   {:puppetlabs.pcp.broker.service/broker-service {:v1 "/pcp/v1.0"
                                                   :vNext "/pcp/vNext"}
    :puppetlabs.trapperkeeper.services.status.status-service/status-service "/status"}

   :metrics {:enabled true
             :server-id "localhost"}})

(defn call-with-broker
  ([f]
   (call-with-broker broker-config f))
  ([config f]
   (with-app-with-config app broker-services config
     (binding [*broker* (-> @(tka/app-context app)
                            :service-contexts
                            :BrokerService
                            :broker)]
       (f)))))

(defmacro with-broker
  [& body]
  `(call-with-broker
     (fn [] ~@body)))
