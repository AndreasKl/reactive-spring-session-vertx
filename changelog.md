# reactive-spring-session-postgres changelog

# 0.0.6
* `ReactivePostgresSessionRepository` publishes metrics if enable. Set `ReactivePostgresSessionRepository#withMetrics` to `true` to activate `Mono#metrics`.
  * The `flow` tag can be configured by setting `ReactivePostgresSessionRepository#setSequenceName`.

# 0.0.1 - 0.0.5
* Initial releases used internally @rewe-digital

