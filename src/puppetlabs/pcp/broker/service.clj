(ns puppetlabs.pcp.broker.service
  (:require [puppetlabs.pcp.broker.core :as core]
            [puppetlabs.pcp.broker.in-memory-inventory :refer [make-inventory record-client find-clients]]
            [puppetlabs.structured-logging.core :as sl]
            [puppetlabs.trapperkeeper.core :as trapperkeeper]
            [puppetlabs.kitchensink.core :as ks]
            [puppetlabs.trapperkeeper.services :refer [service-context get-service]]
            [puppetlabs.trapperkeeper.services.status.status-core :as status-core]
            [puppetlabs.trapperkeeper.services.webserver.jetty9-core :as jetty9-core]
            [puppetlabs.i18n.core :as i18n]))

(trapperkeeper/defservice broker-service
  [[:AuthorizationService authorization-check]
   [:ConfigService get-in-config]
   [:WebroutingService add-websocket-handler get-server get-route]
   [:MetricsService get-metrics-registry]
   [:StatusService register-status]]
  (init [this context]
    (sl/maplog :info {:type :broker-init} (i18n/trs "Initializing broker service"))
    (let [inventory          (make-inventory)
          broker             (core/init {:add-websocket-handler (partial add-websocket-handler this)
                                         :record-client         (partial record-client inventory)
                                         :find-clients          (partial find-clients inventory)
                                         :epoch                 (ks/uuid)
                                         :version               (atom 0)
                                         :authorization-check   authorization-check
                                         :get-metrics-registry  get-metrics-registry
                                         :get-route             (partial get-route this)})]
      (register-status "broker-service"
                       (status-core/get-artifact-version "puppetlabs" "pcp-broker")
                       1
                       (partial core/status broker))
      (assoc context :broker broker)))
  (start [this context]
    (sl/maplog :info {:type :broker-start} (i18n/trs "Starting broker service"))
    (let [broker (:broker context)
          broker-name (or (:broker-name broker)
                          (some-> (get-service this :WebserverService)
                                  service-context
                                  (jetty9-core/get-server-context (keyword (get-server this :v1)))
                                  core/get-webserver-cn)
                          (core/get-localhost-hostname))
          broker (assoc broker :broker-name broker-name)
          context (assoc context :broker broker)]
      (core/start broker)
      (sl/maplog :debug {:type :broker-started :brokername broker-name}
                 (i18n/trs "Broker service <'{brokername}'> started"))
      context))
  (stop [this context]
    (sl/maplog :info {:type :broker-stop} (i18n/trs "Shutting down broker service"))
    (let [broker (:broker context)
          broker-name (:broker-name broker)]
      (core/stop broker)
      (sl/maplog :debug {:type :broker-stopped :brokername broker-name}
                 (i18n/trs "Broker service <'{brokername}'> stopped")))
    context))
