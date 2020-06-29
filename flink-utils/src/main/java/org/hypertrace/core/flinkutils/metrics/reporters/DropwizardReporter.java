package org.hypertrace.core.flinkutils.metrics.reporters;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.dropwizard.metrics.FlinkCounterWrapper;
import org.apache.flink.dropwizard.metrics.FlinkGaugeWrapper;
import org.apache.flink.dropwizard.metrics.FlinkHistogramWrapper;
import org.apache.flink.dropwizard.metrics.FlinkMeterWrapper;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class proxies metrics to the dropwizard metrics registry. Even though there exists a proper
 * flink-metrics-prometheus module that comes with its own shaded version of CollectorRegistry.
 * Which implies there will be 2 instances of CollectorRegistry (one from prometheus client that
 * dropwizard uses and other shaded version from flink). The MetricsServlet can take only one
 * instance of a CollectorRegistry. To address this issue this class acts as a proxy to Dropwizard
 * metric registry. The code is adapted from {@link org.apache.flink.dropwizard.ScheduledDropwizardReporter}.
 * Note ideally it would have been nice to simply sub-class that but the MetricRegistry is a final
 * field that is already instantiated in that class thereby not giving us the option to simply use
 * the PlatformMetricRegistry's instance
 */
public class DropwizardReporter implements MetricReporter, Reporter, CharacterFilter {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropwizardReporter.class);

  // matches anything except the valid characters in square brackets
  private static final Pattern UNALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");

  private final Map<Gauge<?>, String> gauges = new HashMap<>();
  private final Map<Counter, String> counters = new HashMap<>();
  private final Map<Histogram, String> histograms = new HashMap<>();
  private final Map<Meter, String> meters = new HashMap<>();

  @VisibleForTesting
  MetricRegistry registry;

  @Override
  public void open(MetricConfig config) {
    // use the same dropwizard metrics registry as the platform framework does
    this.registry = PlatformMetricsRegistry.getMetricRegistry();
  }

  @Override
  public void close() {
  }

  @Override
  public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
    final String fullName = group.getMetricIdentifier(metricName, this);
    LOGGER.debug("Adding metric=[{}]", fullName);

    synchronized (this) {
      if (metric instanceof Counter) {
        counters.put((Counter) metric, fullName);
        registry.register(fullName, new FlinkCounterWrapper((Counter) metric));
      } else if (metric instanceof Gauge) {
        gauges.put((Gauge<?>) metric, fullName);
        registry.register(fullName, FlinkGaugeWrapper.fromGauge((Gauge<?>) metric));
      } else if (metric instanceof Histogram) {
        Histogram histogram = (Histogram) metric;
        histograms.put(histogram, fullName);

        if (histogram instanceof DropwizardHistogramWrapper) {
          registry.register(fullName,
              ((DropwizardHistogramWrapper) histogram).getDropwizardHistogram());
        } else {
          registry.register(fullName, new FlinkHistogramWrapper(histogram));
        }
      } else if (metric instanceof Meter) {
        Meter meter = (Meter) metric;
        meters.put(meter, fullName);

        if (meter instanceof DropwizardMeterWrapper) {
          registry.register(fullName, ((DropwizardMeterWrapper) meter).getDropwizardMeter());
        } else {
          registry.register(fullName, new FlinkMeterWrapper(meter));
        }
      } else {
        LOGGER.warn("Cannot add metric of type {}. This indicates that the reporter " +
            "does not support this metric type.", metric.getClass().getName());
      }
    }
  }

  @Override
  public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
    synchronized (this) {
      String fullName;

      if (metric instanceof Counter) {
        fullName = counters.remove(metric);
      } else if (metric instanceof Gauge) {
        fullName = gauges.remove(metric);
      } else if (metric instanceof Histogram) {
        fullName = histograms.remove(metric);
      } else if (metric instanceof Meter) {
        fullName = meters.remove(metric);
      } else {
        fullName = null;
      }

      if (fullName != null) {
        LOGGER.debug("Removing metric=[{}]", fullName);
        registry.remove(fullName);
      }
    }
  }

  // https://prometheus.io/docs/instrumenting/writing_exporters/
  // Only [a-zA-Z0-9:_] are valid in metric names, any other characters should be sanitized to an underscore.
  @Override
  public String filterCharacters(String input) {
    return UNALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");
  }

  // ------------------------------------------------------------------------
  //  Getters
  // ------------------------------------------------------------------------

  @VisibleForTesting
  Map<Counter, String> getCounters() {
    return counters;
  }

  @VisibleForTesting
  Map<Meter, String> getMeters() {
    return meters;
  }

  @VisibleForTesting
  Map<Gauge<?>, String> getGauges() {
    return gauges;
  }

  @VisibleForTesting
  Map<Histogram, String> getHistograms() {
    return histograms;
  }
}
