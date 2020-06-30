# reactive-spring-session-vertx changelog

# 0.0.11
* Configure whether a deserialization exception invalidates the session or
  propagates the exception up to the caller.
  * See: net.andreaskluth.session.core.ReactiveVertxSessionRepository.setInvalidateSessionOnDeserializationError
* Support for Java8  
* Various dependency updates:
  * vertx-sql-client : 4.0.0-milestone5

# 0.0.10

# 0.0.9
* Removed jackson serializer, as this was always of limited value
* MySQL support
* Updated Spring Session 2.2.0.RELEASE and Spring Framework 5.2.0.RELEASE

# 0.0.8
* Update to vertx-sql-client 4.0.0-milestone3 to mitigate a direct buffer leak
* Update jackson-databind

# 0.0.7
Fixed metric name and tags.

# 0.0.6
* `ReactivePostgresSessionRepository` publishes metrics if enable. Set `ReactivePostgresSessionRepository#withMetrics` to `true` to activate `Mono#metrics`.
  * The `flow` tag can be configured by setting `ReactivePostgresSessionRepository#setSequenceName`.

# 0.0.1 - 0.0.5
* Initial releases used internally @rewe-digital

