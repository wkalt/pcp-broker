(ns puppetlabs.pcp.broker.service-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [http.async.client :as http]
            [me.raynes.fs :as fs]
            [puppetlabs.pcp.testutils :refer [dotestseq
                                              with-broker
                                              call-with-broker
                                              *broker*
                                              broker-config]]
            [puppetlabs.pcp.testutils.client :as client]
            [puppetlabs.pcp.message :as message]
            [puppetlabs.kitchensink.core :as ks]
            [puppetlabs.trapperkeeper.services.authorization.authorization-service :refer [authorization-service]]
            [puppetlabs.trapperkeeper.services.metrics.metrics-service :refer [metrics-service]]
            [puppetlabs.trapperkeeper.services.scheduler.scheduler-service :refer [scheduler-service]]
            [puppetlabs.trapperkeeper.services.status.status-service :refer [status-service]]
            [puppetlabs.trapperkeeper.services.webrouting.webrouting-service :refer [webrouting-service]]
            [puppetlabs.trapperkeeper.services.webserver.jetty9-service :refer [jetty9-service]]
            [puppetlabs.trapperkeeper.testutils.bootstrap :refer [with-app-with-config]]
            [puppetlabs.trapperkeeper.testutils.logging
             :refer [with-test-logging with-log-level]]
            [puppetlabs.trapperkeeper.app :as tka]
            [slingshot.slingshot :refer [throw+ try+]]
            [schema.test :as st]))

(def protocol-versions
  "The short names of protocol versions"
  ["v1.0" "v2.0"])

<<<<<<< HEAD
(def broker-services
  "The trapperkeeper services the broker needs"
  [authorization-service broker-service jetty9-service webrouting-service metrics-service status-service scheduler-service])

=======
>>>>>>> 5ea3b5d... test version updates and add some test utilities
(deftest it-talks-websockets-test
  (with-broker
    (let [connected (promise)]
      (with-open [client (client/http-client-with-cert "client01.example.com")
                  ws     (http/websocket client
                                         "wss://127.0.0.1:8143/pcp/v2.0"
                                         :open (fn [ws] (deliver connected true)))]
        (is (= true (deref connected (* 2 1000) false)) "Connected within 2 seconds")))))

(deftest it-expects-ssl-client-auth-test
  (with-broker
    (let [closed (promise)]
      (with-open [client (http/create-client)
                  ws (http/websocket client
                                     "wss://127.0.0.1:8143/pcp/v2.0"
                                     :close (fn [ws code reason] (deliver closed code)))]
        ;; NOTE(richardc): This test should only check for close-code 4003, but it
        ;; is a little unreliable and so may sometimes yield the close-code 1006 due
        ;; to a race between the client (netty) becoming connected and the server (jetty)
        ;; closing a connected session because we asked it to.
        ;; This failure is more commonly observed when using Linux
        ;; than on OSX, so we suspect the underlying races to be due
        ;; to thread scheduling.
        ;; See the comments of http://tickets.puppetlabs.com/browse/PCP-124 for more.
        (is (contains? #{ 4003 1006 }
                       (deref closed (* 2 1000) false))
            "Disconnected due to no client certificate")))))

(defn connect-and-close
  "Connect to the broker, wait up to delay ms after connecting
  to see if the broker will close the connection.
  Returns the close code or :refused if the connection was refused"
  [delay]
  (let [close-code (promise)
        connected (promise)]
    (try+
     (with-open [client (client/http-client-with-cert "client01.example.com")
                 ws (http/websocket client
                                    "wss://127.0.0.1:8143/pcp/v2.0"
                                    :open (fn [ws] (deliver connected true))
                                    :close (fn [ws code reason]
                                             (deliver connected false)
                                             (deliver close-code code)))]
       (deref connected)
       ;; We were connected, sleep a while to see if the broker
       ;; disconnects the client.
       (deref close-code delay nil))
     (catch Object _
       (deliver close-code :refused)))
    @close-code))

(defn conj-unique
  "append elem if it is distinct from the last element in the sequence.
  When we port to clojure 1.7 we should be able to use `distinct` on the
  resulting sequence instead of using this on insert."
  [seq elem]
  (if (= (last seq) elem) seq (conj seq elem)))

(defn is-error-message
  "Assert that the message is a PCP error message with the specified description"
  [message version expected-description check-in-reply-to]
  (is (= "http://puppetlabs.com/error_message" (:message_type message)))
  ;; NB(ale): in-reply-to is optional, as it won't be included in case of
  ;; deserialization error
  (if (= "v1.0" version)
    (is (= nil (:in-reply-to message)))
    (when check-in-reply-to
      (is (:in-reply-to message))))
  (is (= expected-description (:description (message/get-json-data message)))))

(defn is-association_response
  "Assert that the associate response matches the canned form."
  [message version]
  (is (= "http://puppetlabs.com/associate_response" (:message_type message)))
  (if (= "v1.0" version)
    (is (= nil (:in-reply-to message)))
    (is (:in-reply-to message)))
  (let [data (message/get-json-data message)]
    (is (true? (:success data)))))

(deftest it-closes-connections-when-not-running-test
  ;; NOTE(richardc): This test is racy.  What we do is we start
  ;; and stop a broker in a future so we can try to connect to it
  ;; while the trapperkeeper services are still starting up.
  (let [should-stop (promise)]
    (try
      (let [broker (future (with-broker
                             ;; Keep the broker alive until the test is done with it.
                                                 (deref should-stop)))
            close-codes (atom [:refused])
            start (System/currentTimeMillis)]
        (while (and (not (future-done? broker)) (< (- (System/currentTimeMillis) start) (* 120 1000)))
          (let [code (connect-and-close (* 20 1000))]
            (when-not (= 1006 code) ;; netty 1006 codes are very racy. Filter out
              (swap! close-codes conj-unique code))
            (if (= 1000 code)
              ;; we were _probably_ able to connect to the broker (or the broker was
              ;; soooo slow to close the connection even though it was not running
              ;; that the 20 seconds timeout expired) so let's tear it down
              (deliver should-stop true))))
        (swap! close-codes conj-unique :refused)
        ;; We expect the following sequence for close codes:
        ;;    :refused (connection refused)
        ;;    1011     (broker not started)
        ;;    1000     (closed because client initated it)
        ;;    1011     (broker stopping)
        ;;    :refused (connection refused)
        ;; though as some of these states may be missed due to timing we test for
        ;; membership of the set of valid sequences.  If we have more
        ;; tests like this it might be worth using ztellman/automat to
        ;; match with a FSM rather than hand-generation of cases.
        (is (contains? #{[:refused 1011 1000 1011 :refused]
                         [:refused 1000 1011 :refused]
                         [:refused 1011 1000 :refused]
                         [:refused 1000 :refused]
                         [:refused 1011 :refused]
                         [:refused]}
                       @close-codes)))
      (finally
        ; security measure to ensure the broker is stopped
        (deliver should-stop true)))))

(deftest poorly-encoded-message-test
  (with-broker
    (dotestseq [version protocol-versions]
      (with-open [client (client/connect :certname "client01.example.com"
                                         :version version)]
        ;; At time of writing, it's deemed very unlikely that any valid
        ;; encoding of a pcp message is a 0-length array.  Sorry future people.
        (client/sendbytes! client (byte-array 0))
        (let [response (client/recv! client)]
          (is-error-message response version "Could not decode message" false))))))

;; Session association tests

(deftest reply-to-malformed-messages-during-association-test
  (testing "During association, broker replies with error_message to a deserialization failure"
    (with-broker
      (dotestseq
        [version protocol-versions]
        (with-open [client (client/connect :certname "client01.example.com"
                                           :modify-association-encoding
                                             (fn [_] (byte-array [0]))
                                           :check-association false
                                           :version version)]
          (let [response (client/recv! client)]
            (is-error-message response version "Could not decode message" false)))))))

(deftest other-messages-are-ignored-during-association-test
  (testing "During association, broker ignores messages other than associate_request"
    (with-broker
      (dotestseq [version protocol-versions]
        (with-open [client (client/connect :certname "client01.example.com"
                                           :modify-association #(assoc % :messate_type "a_message_to_be_ignored")
                                           :check-association false
                                           :version version)]
          (let [response (client/recv! client 500)]
            (is (nil? response))))))))

(deftest certificate-must-match-for-authentication-test
  (testing
    "Unsuccessful associate_response and WebSocket closes if client not authenticated"
    (with-broker
      (dotestseq [version protocol-versions]
                 (with-open [client (client/connect :certname "client01.example.com"
                                                    :uri "pcp://client02.example.com/test"
                                                    :check-association false
                                                    :version version)]
                   (let [pcp-response (client/recv! client)
                         close-websocket-msg (client/recv! client)]
                     (is (is-association_response pcp-response version))))))))

(deftest basic-session-association-test
  (with-broker
    (dotestseq [version protocol-versions]
       ;; NB(ale): client/connect checks associate_response for both clients
      (with-open [client (client/connect :certname "client01.example.com"
                                         :version version)]))))

(deftest expired-session-association-test
  (with-broker
    (dotestseq [version protocol-versions]
      (with-open [client (client/connect :certname "client01.example.com"
                                         :modify-association #(message/set-expiry % -1 :seconds)
                                         :check-association false
                                         :version version)]
        (let [response (client/recv! client)]
          (is (= "http://puppetlabs.com/associate_response" (:message_type response))))))))


;; Inventory service

(deftest inventory-node-can-find-itself-explicit-test
  (with-broker
    (dotestseq [version protocol-versions]
      (with-open [client (client/connect :certname "client01.example.com"
                                         :version version)]
        (let [request (-> (message/make-message)
                          (assoc :message_type "http://puppetlabs.com/inventory_request"
                                 :targets ["pcp:///server"]
                                 :sender "pcp://client01.example.com/test")
                          (message/set-expiry 5 :seconds)
                          (message/set-json-data {:query ["pcp://client01.example.com/test"]}))]
          (client/send! client request)
          (let [response (client/recv! client)
                json-data (message/get-json-data response)]
            (is (= "http://puppetlabs.com/inventory_response" (:message_type response)))
            (is (= (:id request) (:in-reply-to response)))
            (is (= ["pcp://client01.example.com/test"] (:uris json-data)))
            (is (integer? (:version json-data)))))))))

(deftest inventory-node-can-find-itself-wildcard-test
  (with-broker
    (dotestseq [version protocol-versions]
      (with-open [client (client/connect :certname "client01.example.com"
                                         :version version)]
        (let [request (-> (message/make-message)
                          (assoc :message_type "http://puppetlabs.com/inventory_request"
                                 :targets ["pcp:///server"]
                                 :sender "pcp://client01.example.com/test")
                          (message/set-expiry 5 :seconds)
                          (message/set-json-data {:query ["pcp://*/test"]}))]
          (client/send! client request)
          (let [response (client/recv! client)
                {:keys [uris version]} (message/get-json-data response)]
            (is (= "http://puppetlabs.com/inventory_response" (:message_type response)))
            (is (= ["pcp://client01.example.com/test"] uris))
            (is (integer? version))))))))

(deftest inventory-node-cannot-find-previously-connected-node-test
  (with-broker
    (dotestseq [version protocol-versions]
      (with-open [client (client/connect :certname "client02.example.com"
                                         :version version)])
      (with-open [client (client/connect :certname "client01.example.com"
                                         :version version)]
        (let [request (-> (message/make-message)
                          (assoc :message_type "http://puppetlabs.com/inventory_request"
                                 :targets ["pcp:///server"]
                                 :sender "pcp://client01.example.com/test")
                          (message/set-expiry 5 :seconds)
                          (message/set-json-data {:query ["pcp://client02.example.com/test"]}))]
          (client/send! client request))
        (let [response (client/recv! client)
              {:keys [uris version]} (message/get-json-data response)]
          (is (= "http://puppetlabs.com/inventory_response" (:message_type response)))
          (is (= [] uris))
          (is (integer? version)))))))

(def no-inventory-broker-config
  "A broker that allows connections but no inventory requests"
  (assoc-in broker-config [:authorization :rules]
            [{:name "allow all"
              :sort-order 401
              :match-request {:type "regex"
                              :path "^/.*$"}
              :allow-unauthenticated true}
             {:name "deny inventory"
              :sort-order 400
              :match-request {:type "path"
                              :path "/pcp-broker/send"
                              :query-params {:message_type
                                             "http://puppetlabs.com/inventory_request"}}
              :allow []}]))

(deftest authorization-stops-inventory-test
  (call-with-broker
    no-inventory-broker-config
    (fn []
      (dotestseq [version protocol-versions]
                 (with-open [client (client/connect :certname "client01.example.com"
                                                    :version version)]
                   (testing "cannot request inventory"
                     (let [request (-> (message/make-message)
                                       (assoc :message_type "http://puppetlabs.com/inventory_request"
                                              :targets ["pcp:///server"]
                                              :sender "pcp://client01.example.com/test")
                                       (message/set-expiry 5 :seconds)
                                       (message/set-json-data {:query ["pcp://client01.example.com/test"]}))]
                       (client/send! client request)
                       (let [response (client/recv! client)]
                         (is (is-error-message response version "Message not authorized" true))))))))))

(deftest invalid-message-types-not-authorized
  (with-broker
    (dotestseq [version protocol-versions]
      (with-open [client (client/connect :certname "client01.example.com"
                                         :version version)]
        (testing "cannot submit a message with an invalid message_type"
          (with-test-logging
            (let [request (-> (message/make-message)
                              (assoc :message_type "http://puppetlabs.com/inventory_request\u0000"
                                     :targets ["pcp:///server"]
                                     :sender "pcp://client01.example.com/test")
                              (message/set-expiry 5 :seconds))]
              (client/send! client request)
              (let [response (client/recv! client)]
                (is (is-error-message response version "Message not authorized" true))
                (is (logged? #"Illegal message type: 'http://puppetlabs.com/inventory_request" :warn))))))))))

(deftest invalid-targets-not-authorized
  (with-broker
    (dotestseq [version protocol-versions]
      (with-open [client (client/connect :certname "client01.example.com"
                                         :version version)]
        (testing "cannot submit a message with an invalid message_type"
          (with-test-logging
            (let [request (-> (message/make-message)
                              (assoc :message_type "http://puppetlabs.com/inventory_request"
                                     :targets ["pcp:///server\u0000"]
                                     :sender "pcp://client01.example.com/test")
                              (message/set-expiry 5 :seconds))]
              (client/send! client request)
              (let [response (client/recv! client)]
                (is (is-error-message response version "Message not authorized" true))
                (is (logged? #"Illegal message target: 'pcp:///server" :warn))))))))))

;; Message sending

(deftest send-to-self-explicit-test
  (with-broker
    (dotestseq [version protocol-versions]
      (with-open [client (client/connect :certname "client01.example.com"
                                         :version version)]
        (let [message (-> (message/make-message)
                          (assoc :sender "pcp://client01.example.com/test"
                                 :targets ["pcp://client01.example.com/test"]
                                 :message_type "greeting")
                          (message/set-expiry 5 :seconds)
                          (message/set-json-data "Hello"))]
          (client/send! client message)
          (let [message (client/recv! client)]
            (is (= "greeting" (:message_type message)))
            (is (= "Hello" (message/get-json-data message)))))))))

(deftest send-to-self-wildcard-denied-test
  (with-broker
    (dotestseq [version protocol-versions]
      (with-open [client (client/connect :certname "client01.example.com"
                                         :version version)]
        (let [message (-> (message/make-message)
                          (assoc :sender "pcp://client01.example.com/test"
                                 :targets ["pcp://*/test"]
                                 :message_type "greeting")
                          (message/set-expiry 5 :seconds)
                          (message/set-json-data "Hello"))]
          (client/send! client message)
          (let [response (client/recv! client)]
            (is-error-message response version "Multiple recipients no longer supported" false)))))))

(deftest send-with-destination-report-ignored-test
  (with-broker
    (dotestseq [version protocol-versions]
      (with-open [sender   (client/connect :certname "client01.example.com"
                                           :version version)
                  receiver (client/connect :certname "client02.example.com"
                                           :version version)]
        (let [message (-> (message/make-message)
                          (assoc :sender "pcp://client01.example.com/test"
                                 :targets ["pcp://client02.example.com/test"]
                                 :destination_report true
                                 :message_type "greeting")
                          (message/set-expiry 5 :seconds)
                          (message/set-json-data "Hello"))]
          (client/send! sender message)
          (let [message (client/recv! receiver)]
            (is (= "greeting" (:message_type message)))
            (is (= "Hello" (message/get-json-data message)))))))))

(deftest send-expired-test
  (with-broker
    (dotestseq [version protocol-versions]
      (with-open [client (client/connect :certname "client01.example.com"
                                         :version version)]
        (let [message (-> (message/make-message :sender "pcp://client01.example.com/test"
                                                :targets ["pcp://client01.example.com/test"]
                                                :message_type "greeting")
                          (message/set-expiry -1 :seconds))]
          (client/send! client message)
          (let [response (client/recv! client)]
            (is (= "greeting" (:message_type response)))))))))

(deftest send-to-never-connected-will-get-not-connected-test
  (with-broker
    (dotestseq [version protocol-versions]
      (with-open [client (client/connect :certname "client01.example.com"
                                         :version version)]
        (let [message (-> (message/make-message)
                          (assoc :sender "pcp://client01.example.com/test"
                                 :targets ["pcp://client02.example.com/test"]
                                 :message_type "greeting")
                          (message/set-expiry 5 :seconds)
                          (message/set-json-data "Hello"))]
          (client/send! client message)
          (let [response (client/recv! client)]
            (is-error-message response version "not connected" false)))))))

(deftest send-disconnect-connect-not-delivered-test
  (with-broker
    (dotestseq [version protocol-versions]
      (with-open [client1 (client/connect :certname "client01.example.com"
                                          :version version)]
        (let [message (-> (message/make-message)
                          (assoc :sender "pcp://client01.example.com/test"
                                 :targets ["pcp://client02.example.com/test"]
                                 :message_type "greeting")
                          (message/set-expiry 5 :seconds)
                          (message/set-json-data "Hello"))]
          (client/send! client1 message))
        (with-open [client2 (client/connect :certname "client02.example.com")]
          (let [response (client/recv! client1)]
            (is-error-message response version "not connected" false))
          (let [response (client/recv! client2 1000)]
            (is (= nil response))))))))

(def strict-broker-config
  "A broker that only allows test/sensitive message types from client01"
  (assoc-in broker-config [:authorization :rules]
            [{:name "must be client01 to send test/sensitive"
              :sort-order 400
              :match-request {:type "path"
                              :path "/pcp-broker/send"
                              :query-params {:message_type "test/sensitive"}}
              :allow ["client01.example.com"]}
             {:name "allow all"
              :sort-order 420
              :match-request {:type "path"
                              :path "/pcp-broker/send"}
              :allow-unauthenticated true}]))

(deftest authorization-will-stop-some-fun-test
  (call-with-broker
    strict-broker-config
    (fn []
      (dotestseq [version protocol-versions]
                 (with-open [client01 (client/connect :certname "client01.example.com"
                                                      :version version)
                             client02 (client/connect :certname "client02.example.com"
                                                      :version version)]
                   (testing "client01 -> client02 should work"
                     (let [message (-> (message/make-message
                                         :sender "pcp://client01.example.com/test"
                                         :message_type "test/sensitive"
                                         :targets ["pcp://client02.example.com/test"])
                                       (message/set-expiry 5 :seconds))]
                       (client/send! client01 message)
                       (let [received (client/recv! client02)]
                         (is (= (:id message) (:id received))))))
                   (testing "client02 -> client01 should not work"
                     (let [message (-> (message/make-message
                                         :sender "pcp://client02.example.com/test"
                                         :message_type "test/sensitive"
                                         :targets ["pcp://client01.example.com/test"])
                                       (message/set-expiry 5 :seconds))]
                       (client/send! client02 message)
                       (let [received (client/recv! client01 1000)]
                         (is (= nil received))))))))))

(deftest version-updates-test
  (with-broker
    (is (zero? @(:version *broker*)))
    (with-open [con (client/connect :certname "client01.example.com"
                                    :version "v2.0")]
      (is (= 1 @(:version *broker*))))
    (is (= 2 @(:version *broker*)))))

(deftest interversion-send-test
  (with-broker
    (dotestseq [sender-version   ["v1.0" "v2.0" "v1.0" "v2.0"]
                receiver-version ["v1.0" "v1.0" "v2.0" "v2.0"]]
      (with-open [sender   (client/connect :certname "client01.example.com"
                                           :version sender-version)
                  receiver (client/connect :certname "client02.example.com"
                                           :version receiver-version)]
        (let [message (-> (message/make-message)
                          (assoc :sender "pcp://client01.example.com/test"
                                 :targets ["pcp://client02.example.com/test"]
                                 :in-reply-to (ks/uuid)
                                 :message_type "greeting")
                          (message/set-expiry 5 :seconds)
                          (message/set-json-data "Hello"))]
          (client/send! sender message)
          (let [received-msg (client/recv! receiver)]
            (is (= (case receiver-version
                     "v1.0" nil
                     (:in-reply-to message))
                   (:in-reply-to received-msg)))
            (is (= "greeting" (:message_type received-msg)))
            (is (= "Hello" (message/get-json-data received-msg)))))))))
