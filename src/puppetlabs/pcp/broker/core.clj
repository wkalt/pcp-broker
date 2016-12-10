(ns puppetlabs.pcp.broker.core
  (:require [metrics.gauges :as gauges]
            [puppetlabs.experimental.websockets.client :as websockets-client]
            [puppetlabs.pcp.broker.connection :as connection :refer [Websocket Codec ConnectionState]]
            [puppetlabs.pcp.broker.metrics :as metrics]
            [puppetlabs.pcp.message :as message :refer [Message]]
            [puppetlabs.pcp.protocol :as p]
            [puppetlabs.metrics :refer [time!]]
            [puppetlabs.ssl-utils.core :as ssl-utils]
            [puppetlabs.structured-logging.core :as sl]
            [puppetlabs.trapperkeeper.authorization.ring :as ring]
            [puppetlabs.trapperkeeper.services.status.status-core :as status-core]
            [puppetlabs.trapperkeeper.services.webserver.jetty9-core :as jetty9-core]
            [schema.core :as s]
            [slingshot.slingshot :refer [throw+ try+]]
            [puppetlabs.i18n.core :as i18n])
  (:import [puppetlabs.pcp.broker.connection Connection]
           [clojure.lang IFn Atom]
           [java.util.concurrent ConcurrentHashMap]
           [java.net InetAddress UnknownHostException]
           [java.security KeyStore]))

(def Broker
  {:broker-name        (s/maybe s/Str)
   :record-client      IFn
   :find-clients       IFn
   :authorization-check IFn
   :uri-map            ConcurrentHashMap ;; Mapping of Uri to Websocket, for sending
   :connections        ConcurrentHashMap ;; Mapping of Websocket session to Connection state
   :inventory          ConcurrentHashMap ;; Mapping of Uri to websocket session
   :version            Atom
   :epoch              s/Str
   :metrics-registry   Object
   :metrics            {s/Keyword Object}
   :state              Atom})

(s/defn build-and-register-metrics :- {s/Keyword Object}
  [broker :- Broker]
  (let [registry (:metrics-registry broker)]
    (gauges/gauge-fn registry ["puppetlabs.pcp.connections"]
                     (fn [] (count (:inventory broker))))
    {:on-connect       (.timer registry "puppetlabs.pcp.on-connect")
     :on-close         (.timer registry "puppetlabs.pcp.on-close")
     :on-message       (.timer registry "puppetlabs.pcp.on-message")
     :message-queueing (.timer registry "puppetlabs.pcp.message-queueing")
     :on-send          (.timer registry "puppetlabs.pcp.on-send")}))

(defn get-certificate-chain
  "Return the first non-empty certificate chain encountered while scanning the
  entries in the key store of the specified org.eclipse.jetty.util.ssl.SslContextFactory
  instance - `ssl-context-factory`."
  [ssl-context-factory]
  (try
    (let [load-key-store-method (doto
                                  (-> (class ssl-context-factory)
                                      (.getDeclaredMethod "loadKeyStore" (into-array Class [])))
                                  (.setAccessible true))
          ^KeyStore key-store (.invoke load-key-store-method ssl-context-factory (into-array Object []))]
      (->> (.aliases key-store)
           enumeration-seq
           (some #(.getCertificateChain key-store %))))
    (catch Exception _)))

(s/defn get-webserver-cn :- (s/maybe s/Str)
  "Return the common name from the certificate the webserver specified by its
  context - `webserver-context` - will use when establishing SSL connections
  or nil if there was a problem finding out the certificate (for instance
  when the webserver is not SSL enabled)."
  [webserver-context :- jetty9-core/ServerContext]
  (some-> webserver-context
          :state
          deref
          :ssl-context-factory
          get-certificate-chain
          first
          ssl-utils/get-cn-from-x509-certificate))

(s/defn get-localhost-hostname :- s/Str
  "Return the hostname of the host executing the code."
  []
  (try
    (-> (InetAddress/getLocalHost)
        .getHostName)
    (catch UnknownHostException e
      (let [message (.getMessage e)]
        (subs message 0 (.indexOf message (int \:)))))))

(s/defn broker-uri :- p/Uri
  [broker :- Broker]
  (str "pcp://" (:broker-name broker) "/server"))

;; connection map lifecycle
(s/defn add-connection! :- Connection
  "Add a Connection to the connections to track a websocket"
  [broker :- Broker ws :- Websocket codec :- Codec]
  (let [connection (connection/make-connection ws codec)]
    (.put (:connections broker) ws connection)
    (swap! (:version broker) inc)
    connection))

(s/defn remove-connection!
  "Remove tracking of a Connection from the broker by websocket"
  [broker :- Broker ws :- Websocket]
  (if-let [uri (get-in (:connections broker) [ws :uri])]
    (.remove (:uri-map broker) uri))
  (swap! (:version broker) inc)
  (.remove (:connections broker) ws))

(s/defn get-connection :- (s/maybe Connection)
  [broker :- Broker ws :- Websocket]
  (get (:connections broker) ws))

(s/defn get-websocket :- (s/maybe Websocket)
  "Return the websocket a node identified by a uri is connected to, false if not connected"
  [broker :- Broker uri :- p/Uri]
  (get (:uri-map broker) uri))

;;
;; Message processing
;;

(def MessageLog
  "Schema for a loggable summary of a message"
  {:messageid p/MessageId
   :source s/Str
   :messagetype s/Str
   :destination [p/Uri]})

(s/defn summarize :- MessageLog
  [message :- Message]
  {:messageid (:id message)
   :messagetype (:message_type message)
   :source (:sender message)
   :destination (:targets message)})

;; message lifecycle

(s/defn make-error-data-content :- p/ErrorMessage
  [in-reply-to-message description]
  (let [data-content {:description description}
        data-content (if in-reply-to-message
                       (assoc data-content :id (:id in-reply-to-message))
                       data-content)]
    data-content))

(s/defn send-error-message
  [in-reply-to-message :- (s/maybe Message) description :- s/Str connection :- Connection]
  (let [data-content (make-error-data-content in-reply-to-message description)
        error-msg (-> (message/make-message :message_type "http://puppetlabs.com/error_message"
                                            :sender "pcp:///server")
                      (message/set-json-data data-content))
        error-msg (if in-reply-to-message
                    (assoc error-msg :in-reply-to (:id in-reply-to-message))
                    error-msg)
        {:keys [codec websocket]} connection
        encode (:encode codec)]
    (websockets-client/send! websocket (encode error-msg))
    nil))

;; NB(michael): using (s/maybe Connection) in the signature for the sake of testing
(s/defn handle-delivery-failure
  "Send an error message with the specified description."
  [broker :- Broker message :- Message sender :- (s/maybe Connection) reason :- s/Str]
  (sl/maplog :trace (assoc (summarize message)
                           :type :message-delivery-failure
                           :reason reason)
             (i18n/trs "Failed to deliver '{messageid}' for '{destination}': '{reason}'"))
  (send-error-message message reason sender))

(s/defn multicast-message?
  "Returns a boolean specifying whether the message uses multicast in the target field."
  [message :- Message]
  (if-let [targets (:targets message)]
    (if-let [first-target (first targets)]
      (or (not= 1 (count targets)) (p/uri-wildcard? first-target)))))

;; NB(michael): using (s/maybe Connection) in the signature for the sake of testing
(s/defn deliver-message
  "Message consumer. Delivers a message to the websocket indicated by the :targets field"
  [broker :- Broker
   message :- Message
   sender :- (s/maybe Connection)]
  (assert (not (multicast-message? message)))
  (if-let [websocket (get-websocket broker (first (:targets message)))]
    (try
      (let [connection (get-connection broker websocket)
            encode (get-in connection [:codec :encode])]
        (locking websocket
          (sl/maplog
            :debug (merge (summarize message)
                          (connection/summarize connection)
                          {:type :message-delivery})
            (i18n/trs "Delivering '{messageid}' for '{destination}' to '{commonname}' at '{remoteaddress}'"))
          (time! (:on-send (:metrics broker))
                 (websockets-client/send! websocket (encode message)))))
      (catch Exception e
        (sl/maplog :error e
                   {:type :message-delivery-error}
                   (i18n/trs "Error in deliver-message"))
        (handle-delivery-failure broker message sender (str e))))
    (handle-delivery-failure broker message sender (i18n/trs "not connected"))))

(s/defn session-association-request? :- s/Bool
  "Return true if message is a session association message"
  [message :- Message]
  (and (= (:targets message) ["pcp:///server"])
       (= (:message_type message) "http://puppetlabs.com/associate_request")))

;; process-associate-request! helper
(s/defn reason-to-deny-association :- (s/maybe s/Str)
  "Returns an error message describing why the session should not be
  allowed, if it should be denied"
  [broker :- Broker
   connection :- Connection
   as :- p/Uri]
  (let [[_ type] (p/explode-uri as)]
    (cond
      (= type "server")
      (i18n/trs "''server'' type connections not accepted")
      (= :associated (:state connection))
      (let [{:keys [uri]} connection]
        (sl/maplog
          :debug (assoc (connection/summarize connection)
                        :uri as
                        :existinguri uri
                        :type :connection-already-associated)
          (i18n/trs "Received session association for '{uri}' from '{commonname}' '{remoteaddress}'. Session was already associated as '{existinguri}'"))
        (i18n/trs "Session already associated")))))

(s/defn make-associate_response-data-content :- p/AssociateResponse
  [id reason-to-deny]
  (if reason-to-deny
    {:id id :success false :reason reason-to-deny}
    {:id id :success true}))

(s/defn process-associate-request! :- (s/maybe Connection)
  "Send an associate_response that will be successful if:
    - a reason-to-deny is not specified as an argument nor determined by
      reason-to-deny-association;
    - the requester `client_type` is not `server`;
    - the specified WebSocket connection has not been associated previously.
  If the request gets denied, the WebSocket connection will be closed and the
  function returns nil.
  Otherwise, the 'Connection' object's state will be marked as associated and
  returned. Also, in case another WebSocket connection with the same client
  is currently associated, such old connection will be superseded by the new
  one (i.e. the old connection will be closed by the brocker).

  Note that this function will not update the broker by removing the connection
  from the 'connections' map, nor the 'uri-map'. It is assumed that such update
  will be done asynchronously by the onClose handler."
  ;; TODO(ale): make associate_request idempotent when succeed (PCP-521)
  ([broker :- Broker
    message :- Message
    connection :- Connection]
    (let [requester-uri (:sender message)
          reason-to-deny (reason-to-deny-association broker connection requester-uri)]
      (process-associate-request! broker message connection reason-to-deny)))
  ([broker :- Broker request :- Message connection :- Connection reason-to-deny :- (s/maybe s/Str)]
    ;; NB(ale): don't validate the associate_request as there's no data chunk...
    (let [ws (:websocket connection)
          id (:id request)
          encode (get-in connection [:codec :encode])
          requester-uri (:sender request)
          response-data (make-associate_response-data-content id reason-to-deny)]
      (let [message (-> (message/make-message :message_type "http://puppetlabs.com/associate_response"
                                              :targets [requester-uri]
                                              :in-reply-to id
                                              :sender "pcp:///server")
                        (message/set-json-data response-data)
                        (message/set-expiry 3 :seconds))]
        (sl/maplog :debug {:type :associate_response-trace
                           :requester requester-uri
                           :rawmsg message}
                   (i18n/trs "Replying to '{requester}' with associate_response: '{rawmsg}'"))
        (websockets-client/send! ws (encode message)))
      (if reason-to-deny
        (do
          (sl/maplog
            :debug {:type   :connection-association-failed
                    :uri    requester-uri
                    :reason reason-to-deny}
            (i18n/trs "Invalid associate_request ('{reason}'); closing '{uri}' WebSocket"))
          (websockets-client/close! ws 4002 (i18n/trs "association unsuccessful"))
          nil)
        (let [{:keys [uri-map record-client]} broker]
          (when-let [old-ws (get-websocket broker requester-uri)]
            (let [connections (:connections broker)]
              (sl/maplog
                :debug (assoc (connection/summarize connection)
                         :uri requester-uri
                         :type :connection-association-failed)
                (i18n/trs "Node with URI '{uri}' already associated with connection '{commonname}' '{remoteaddress}'"))
              (websockets-client/close! old-ws 4000 (i18n/trs "superseded"))
              (.remove connections old-ws)))
          (.put uri-map requester-uri ws)
          (record-client requester-uri)
          (assoc connection
            :uri requester-uri
            :state :associated))))))

(s/defn make-inventory_response-data-content :- p/InventoryResponse
  [{:keys [find-clients epoch version] :as broker} {:keys [query]}]
  (let [uris (doall (filter (partial get-websocket broker) (find-clients query)))]
    {:uris uris
     :version (str epoch "_" @version)}))

(s/defn process-inventory-request
  "Process a request for inventory data.
   This function assumes that the requester client is associated.
   Returns nil."
  [broker :- Broker
   message :- Message
   connection :- Connection]
  (assert (= (:state connection) :associated))
  (let [data (message/get-json-data message)]
    (s/validate p/InventoryRequest data)
    (let [response-data (make-inventory_response-data-content broker data)]
      (deliver-message
        broker
        (-> (message/make-message :message_type "http://puppetlabs.com/inventory_response"
                                  :targets [(:sender message)]
                                  :in-reply-to (:id message)
                                  :sender "pcp:///server")
            (message/set-json-data response-data)
            ;; set expiration last so if any of the previous steps take
            ;; significant time the message doesn't expire
            (message/set-expiry 3 :seconds))
        connection)))
  nil)

(s/defn process-server-message! :- (s/maybe Connection)
  "Process a message directed at the middleware"
  [broker :- Broker message :- Message connection :- Connection]
  (let [message-type (:message_type message)]
    (case message-type
      "http://puppetlabs.com/associate_request" (process-associate-request! broker message connection)
      "http://puppetlabs.com/inventory_request" (process-inventory-request broker message connection)
      (do
        (sl/maplog
          :debug (assoc (connection/summarize connection)
                   :messagetype message-type
                   :type :broker-unhandled-message)
          (i18n/trs "Unhandled message type '{messagetype}' received from '{commonname}' '{remoteaddr}'"))))))

;;
;; Message validation
;;

(defn- validate-message-type
  [^String message-type]
  (if-not (re-matches #"^[\w\-.:/]*$" message-type)
    (i18n/trs "Illegal message type: ''{0}''" message-type)))

(defn- validate-target
  [^String target]
  (if-not (re-matches #"^[\w\-.:/*]*$" target)
    (i18n/trs "Illegal message target: ''{0}''" target)))

(s/defn make-ring-request :- (s/maybe ring/Request)
  [message :- Message connection :- (s/maybe Connection)]
  (let [{:keys [sender targets message_type destination_report]} message]
    (if-let [validation-result (or (validate-message-type message_type) (some validate-target targets))]
      (do
        (sl/maplog
          :warn (merge (summarize message)
                       {:type    :message-authorization
                        :allowed false
                        :message validation-result})
          (i18n/trs "Message '{messageid}' for '{destination}' didn''t pass pre-authorization validation: '{message}'"))
        nil)                                                ; make sure to return nil
      (let [query-params {"sender" sender
                          "targets" (if (= 1 (count targets)) (first targets) targets)
                          "message_type" message_type
                          "destination_report" (boolean destination_report)}
            request {:uri            "/pcp-broker/send"
                     :request-method :post
                     :remote-addr    ""
                     :form-params    {}
                     :query-params   query-params
                     :params         query-params}]
        ;; NB(ale): we may not have the Connection when running tests
        (if connection
          (let [remote-addr (:remote-address connection)
                ssl-client-cert (first (websockets-client/peer-certs (:websocket connection)))]
            (assoc request :remote-addr remote-addr
                           :ssl-client-cert ssl-client-cert))
          request)))))

;; NB(ale): using (s/maybe Connection) in the signature for the sake of testing
(s/defn authorized? :- s/Bool
  "Check if the message within the specified message is authorized"
  [broker :- Broker request :- Message connection :- (s/maybe Connection)]
  (if-let [ring-request (make-ring-request request connection)]
    (let [{:keys [authorization-check]} broker
          {:keys [authorized message]} (authorization-check ring-request)
          allowed (boolean authorized)]
      (sl/maplog
        :trace (merge (summarize request)
                      {:type    :message-authorization
                       :allowed allowed
                       :auth-message message})
        (i18n/trs "Authorizing '{messageid}' for '{destination}' - '{allowed}': '{auth-message}'"))
      allowed)
    false))

(s/defn authenticated? :- s/Bool
  "Check if the cert name advertised by the sender of the message contained in
   the specified Message matches the cert name in the certificate of the
   given Connection"
  [message :- Message connection :- Connection]
  (let [{:keys [common-name]} connection
        sender (:sender message)
        [client] (p/explode-uri sender)]
    (= client common-name)))

(def MessageValidationOutcome
  "Outcome of validate-message"
  (s/enum :to-be-ignored-during-association
          :not-authenticated
          :not-authorized
          :multicast-unsupported
          :to-be-processed))

(s/defn validate-message :- MessageValidationOutcome
  "Determine whether the specified message should be processed by checking,
   in order, if the message: 1) is an associate-request as expected during
   Session Association; 2) is authenticated; 3) is authorized; 4) does not
   use multicast delivery."
  [broker :- Broker message :- Message connection :- Connection is-association-request :- s/Bool]
  (cond
    (and (= :open (:state connection))
         (not is-association-request)) :to-be-ignored-during-association
    (not (authenticated? message connection)) :not-authenticated
    (not (authorized? broker message connection)) :not-authorized
    (multicast-message? message) :multicast-unsupported
    :else :to-be-processed))

;;
;; WebSocket onMessage handling
;;

(defn log-access
  [lvl message-data]
  (sl/maplog
    [:puppetlabs.pcp.broker.pcp_access lvl]
    message-data
    "{accessoutcome} {remoteaddress} {commonname} {source} {messagetype} {messageid} {destination}"))

;; NB(ale): using (s/maybe Websocket) in the signature for the sake of testing
(s/defn process-message! :- (s/maybe Connection)
  "Deserialize, validate (authentication, authorization, and expiration), and
  process the specified raw message. Return the 'Connection' object associated
  to the specified 'Websocket' in case it gets modified (hence the '!' in the
  function name), otherwise nil.
  Also, log the message validation outcome via 'pcp-access' logger."
  [broker :- Broker
   bytes :- message/ByteArray
   ws :- (s/maybe Websocket)]
  (let [connection (get-connection broker ws)
        decode (get-in connection [:codec :decode])]
    (try+
      (let [message (decode bytes)
            message-data (merge (connection/summarize connection)
                                (summarize message))
            is-association-request (session-association-request? message)]
        (sl/maplog :trace {:type :incoming-message-trace :rawmsg message}
                   (i18n/trs "Processing PCP message: '{rawmsg}'"))
        (try+
          (case (validate-message broker message connection is-association-request)
            :to-be-ignored-during-association
            (log-access :warn (assoc message-data :accessoutcome "IGNORED_DURING_ASSOCIATION"))
            :not-authenticated
            (let [not-authenticated-msg (i18n/trs "Message not authenticated")]
              (log-access :warn (assoc message-data :accessoutcome "AUTHENTICATION_FAILURE"))
              (if is-association-request
                ;; send an unsuccessful associate_response and close the WebSocket
                (process-associate-request! broker message connection not-authenticated-msg)
                (send-error-message message not-authenticated-msg connection)))
            :not-authorized
            (let [not-authorized-msg (i18n/trs "Message not authorized")]
              (log-access :warn (assoc message-data :accessoutcome "AUTHORIZATION_FAILURE"))
              (if is-association-request
                ;; send an unsuccessful associate_response and close the WebSocket
                (process-associate-request! broker message connection not-authorized-msg)
                ;; TODO(ale): use 'unauthorized' in version 2
                (send-error-message message not-authorized-msg connection)))
            :multicast-unsupported
            (let [multicast-unsupported-message (i18n/trs "Multiple recipients no longer supported")]
              (log-access :warn (assoc message-data :accessoutcome "MULTICAST_UNSUPPORTED"))
              (send-error-message message multicast-unsupported-message connection))
            :to-be-processed
            (do
              (log-access :info (assoc message-data :accessoutcome "AUTHORIZATION_SUCCESS"))
              (if (= (:targets message) ["pcp:///server"])
                (process-server-message! broker message connection)
                (do
                  (assert (= (:state connection) :associated))
                  (deliver-message broker message connection)
                  nil)))
            ;; default case
            (assert false (i18n/trs "unexpected message validation outcome")))
          (catch map? m
            (sl/maplog
              :debug (merge (connection/summarize connection)
                            (summarize message)
                            {:type :processing-error :errortype (:type m)})
              (i18n/trs "Failed to process '{messagetype}' '{messageid}' from '{commonname}' '{remoteaddress}': '{errortype}'"))
            (send-error-message
              message
              (i18n/trs "Error {0} handling message: {1}" (:type m) &throw-context)
              connection))))
      (catch map? m
        (sl/maplog
          [:puppetlabs.pcp.broker.pcp_access :warn]
          (assoc (connection/summarize connection) :type :deserialization-failure
                                                   :outcome "DESERIALIZATION_ERROR")
          "{outcome} {remoteaddress} {commonname} unknown unknown unknown unknown unknown")
        ;; TODO(richardc): this could use a different message_type to
        ;; indicate an encoding error rather than a processing error
        (send-error-message nil (i18n/trs "Could not decode message") connection)))))

(defn on-message!
  "If the broker service is not running, close the WebSocket connection.
   Otherwise process the message and, in case a 'Connection' object is
   returned, updates the related broker's 'connections' map entry."
  [broker ws bytes]
  (time!
    (:on-message (:metrics broker))
    (if-not (= :running @(:state broker))
      (websockets-client/close! ws 1011 (i18n/trs "Broker is not running"))
      (when-let [connection (process-message! broker bytes ws)]
        (assert (instance? Connection connection))
        (.put (:connections broker) ws connection)))))

(defn- on-text!
  "OnMessage (text) websocket event handler"
  [broker ws message]
  (on-message! broker ws (message/string->bytes message)))

(defn- on-bytes!
  "OnMessage (binary) websocket event handler"
  [broker ws bytes offset len]
  (on-message! broker ws bytes))

;;
;; Other WebSocket event handlers
;;

(defn- on-connect!
  "OnOpen WebSocket event handler. Close the WebSocket connection if the
   Broker service is not running or if the client common name is not obtainable
   from its cert. Otherwise set the idle timeout of the WebSocket connection
   to 15 min."
  [broker codec ws]
  (time!
    (:on-connect (:metrics broker))
    (if-not (= :running @(:state broker))
      (websockets-client/close! ws 1011 (i18n/trs "Broker is not running"))
      (let [connection (add-connection! broker ws codec)
            {:keys [common-name]} connection
            idle-timeout (* 1000 60 15)]
        (if (nil? common-name)
          (do
            (sl/maplog :debug (assoc (connection/summarize connection)
                                :type :connection-no-peer-certificate)
                       (i18n/trs "No client certificate, closing '{remoteaddress}'"))
            (websockets-client/close! ws 4003 (i18n/trs "No client certificate")))
          (do
            (websockets-client/idle-timeout! ws idle-timeout)
            (sl/maplog :debug (assoc (connection/summarize connection)
                                :type :connection-open)
                       (i18n/trs "client '{commonname}' connected from '{remoteaddress}'"))))))))

(defn- on-error
  "OnError WebSocket event handler. Just log the event."
  [broker ws e]
  (let [connection (get-connection broker ws)]
    (sl/maplog :error e (assoc (connection/summarize connection)
                               :type :connection-error)
               (i18n/trs "Websocket error '{commonname}' '{remoteaddress}'"))))

(defn- on-close!
  "OnClose WebSocket event handler. Remove the Connection instance out of the
   broker's 'connections' map."
  [broker ws status-code reason]
  (time! (:on-close (:metrics broker))
         (let [connection (get-connection broker ws)]
           (sl/maplog
             :debug (assoc (connection/summarize connection)
                           :type :connection-close
                           :statuscode status-code
                           :reason reason)
             (i18n/trs "client '{commonname}' disconnected from '{remoteaddress}' '{statuscode}' '{reason}'"))
           (remove-connection! broker ws)
           (.remove (:inventory broker) ws))))

(s/defn build-websocket-handlers :- {s/Keyword IFn}
  [broker :- Broker
   codec]
  {:on-connect (partial on-connect! broker codec)
   :on-error   (partial on-error broker)
   :on-close   (partial on-close! broker)
   :on-text    (partial on-text! broker)
   :on-bytes   (partial on-bytes! broker)})

;;
;; Broker service lifecycle, codecs, status service
;;

(def InitOptions
  {:add-websocket-handler IFn
   :epoch s/Str
   :version Atom
   :record-client IFn
   :find-clients IFn
   :authorization-check IFn
   :get-metrics-registry IFn
   :get-route IFn
   (s/optional-key :broker-name) s/Str})

(s/def v1-codec :- Codec
  "Codec for handling v1.0 messages. Strip out :in-reply-to for all messages
   except inventory responses, and append the agent client_type to messages
   that do not specify a client_type."
  {:decode message/decode
   :encode (fn [message]
             (let [message_type (:message_type message)
                   [_ type] (p/explode-uri (:sender message))
                   message (cond-> message
                             (not= "http://puppetlabs.com/inventory_response" message_type) (dissoc :in-reply-to)
                             (nil? type) (update :sender str "/agent"))]
               (message/encode message)))})

(s/def v2-codec :- Codec
  {:decode message/decode
   :encode message/encode})

(s/defn init :- Broker
  [options :- InitOptions]
  (let [{:keys [broker-name
                add-websocket-handler
                record-client
                find-clients
                authorization-check
                get-route version epoch
                get-metrics-registry ssl-cert]} options
        broker {:broker-name broker-name
                :record-client record-client
                :find-clients find-clients
                :authorization-check authorization-check
                :version version
                :epoch epoch
                :metrics {}
                :metrics-registry (get-metrics-registry)
                :connections (ConcurrentHashMap.)
                :uri-map (ConcurrentHashMap.)
                :inventory (ConcurrentHashMap.)
                :state (atom :starting)}
        metrics (build-and-register-metrics broker)
        broker (assoc broker :metrics metrics)]
    (add-websocket-handler
      (build-websocket-handlers broker v1-codec) {:route-id :v1})
    (add-websocket-handler
      (build-websocket-handlers broker v2-codec) {:route-id :v2})
    broker))

(s/defn start
  [{:keys [state]} :- Broker]
  (reset! state :running))

(s/defn stop
  [{:keys [state]} :- Broker]
  (reset! state :stopping))

(s/defn status :- status-core/StatusCallbackResponse
  [broker :- Broker
   level :- status-core/ServiceStatusDetailLevel]
  (let [{:keys [state metrics-registry]} broker
        level>= (partial status-core/compare-levels >= level)]
    {:state  @state
     :status (cond-> {}
               (level>= :info) (assoc :metrics (metrics/get-pcp-metrics metrics-registry))
               (level>= :debug) (assoc :threads (metrics/get-thread-metrics)
                                       :memory (metrics/get-memory-metrics)))}))
