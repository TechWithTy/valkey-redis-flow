Integrating OpenTelemetry
What is OpenTelemetry?

OpenTelemetry is an open-source observability framework for traces, metrics, and logs. It is a merger of OpenCensus and OpenTracing projects hosted by Cloud Native Computing Foundation.

OpenTelemetry allows developers to collect and export telemetry data in a vendor agnostic way. With OpenTelemetry, you can instrument your application once and then add or change vendors without changing the instrumentation, for example, here is a list of popular DataDog competitors that support OpenTelemetry.
What is tracing?

OpenTelemetry tracing allows you to see how a request progresses through different services and systems, timings of each operation, any logs and errors as they occur.

In a distributed environment, tracing also helps you understand relationships and interactions between microservices. Distributed tracing gives an insight into how a particular microservice is performing and how that service affects other microservices.
Trace

Using tracing, you can break down requests into spans. Span is an operation (unit of work) your app performs handling a request, for example, a database query or a network call.

Trace is a tree of spans that shows the path that a request makes through an app. Root span is the first span in a trace.
Trace

To learn more about tracing, see Distributed Tracing using OpenTelemetry.
OpenTelemetry instrumentation

Instrumentations are plugins for popular frameworks and libraries that use OpenTelemetry API to record important operations, for example, HTTP requests, DB queries, logs, errors, and more.

To install OpenTelemetry instrumentation for valkey-py:

pip install opentelemetry-instrumentation-valkey

You can then use it to instrument code like this:

from opentelemetry.instrumentation.valkey import ValkeyInstrumentor

ValkeyInstrumentor().instrument()

Once the code is patched, you can use valkey-py as usually:

# Sync client
client = valkey.Valkey()
client.get("my-key")

# Async client
client = valkey.asyncio.Valkey()
await client.get("my-key")

OpenTelemetry API

OpenTelemetry API is a programming interface that you can use to instrument code and collect telemetry data such as traces, metrics, and logs.

You can use OpenTelemetry API to measure important operations:

from opentelemetry import trace

tracer = trace.get_tracer("app_or_package_name", "1.0.0")

# Create a span with name "operation-name" and kind="server".
with tracer.start_as_current_span("operation-name", kind=trace.SpanKind.CLIENT) as span:
    do_some_work()

Record contextual information using attributes:

if span.is_recording():
    span.set_attribute("http.method", "GET")
    span.set_attribute("http.route", "/projects/:id")

And monitor exceptions:

except ValueError as exc:
    # Record the exception and update the span status.
    span.record_exception(exc)
    span.set_status(trace.Status(trace.StatusCode.ERROR, str(exc)))

See OpenTelemetry Python Tracing API for details.
Uptrace

Uptrace is an open source APM that supports distributed tracing, metrics, and logs. You can use it to monitor applications and set up automatic alerts to receive notifications via email, Slack, Telegram, and more.

You can use Uptrace to monitor valkey-py using this GitHub example as a starting point.
Valkey-py trace

You can install Uptrace by downloading a DEB/RPM package or a pre-compiled binary.
Monitoring Valkey Server performance

In addition to monitoring valkey-py client, you can also monitor Valkey Server performance using OpenTelemetry Collector Agent.

OpenTelemetry Collector is a proxy/middleman between your application and a distributed tracing tool such as Uptrace or Jaeger. Collector receives telemetry data, processes it, and then exports the data to APM tools that can store it permanently.

For example, you can use the OpenTelemetry Valkey receiver <https://uptrace.dev/get/monitor/opentelemetry-valkey.html> provided by Otel Collector to monitor Valkey performance:
Valkey metrics

See introduction to OpenTelemetry Collector for details.
Alerting and notifications

Uptrace also allows you to monitor OpenTelemetry metrics using alerting rules. For example, the following monitor uses the group by node expression to create an alert whenever an individual Valkey shard is down:

monitors:
  - name: Valkey shard is down
    metrics:
      - valkey_up as $valkey_up
    query:
      - group by cluster # monitor each cluster,
      - group by bdb # each database,
      - group by node # and each shard
      - $valkey_up
    min_allowed_value: 1
    # shard should be down for 5 minutes to trigger an alert
    for_duration: 5m

You can also create queries with more complex expressions. For example, the following rule creates an alert when the keyspace hit rate is lower than 75%:

monitors:
  - name: Valkey read hit rate < 75%
    metrics:
      - valkey_keyspace_read_hits as $hits
      - valkey_keyspace_read_misses as $misses
    query:
      - group by cluster
      - group by bdb
      - group by node
      - $hits / ($hits + $misses) as hit_rate
    min_allowed_value: 0.75
    for_duration: 5m

