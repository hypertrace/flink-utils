package org.hypertrace.core.flinkutils.metrics.reporters;

import com.codahale.metrics.MetricRegistry;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.util.AbstractID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DropwizardReporterTest {

  @Test
  public void invalidCharactersAreReplacedWithUnderscore() {
    DropwizardReporter reporter = new DropwizardReporter();

    assertEquals("", reporter.filterCharacters(""));
    assertEquals("abc", reporter.filterCharacters("abc"));
    assertEquals("abc_", reporter.filterCharacters("abc\""));
    assertEquals("_abc", reporter.filterCharacters("\"abc"));
    assertEquals("_abc_", reporter.filterCharacters("\"abc\""));
    assertEquals("_a_b_c_", reporter.filterCharacters("\"a\"b\"c\""));
    assertEquals("____", reporter.filterCharacters("\"\"\"\""));
    assertEquals("____", reporter.filterCharacters("    "));
    assertEquals("_ab___c__", reporter.filterCharacters("\"ab ;(c)'"));
    assertEquals("a_b_c", reporter.filterCharacters("a b c"));
    assertEquals("a_b_c_", reporter.filterCharacters("a b c "));
    assertEquals("a_b_c_", reporter.filterCharacters("a;b'c*"));
    assertEquals("a___:__b___:__c", reporter.filterCharacters("a,=;:?'b,=;:?'c"));
    assertEquals("abc", reporter.filterCharacters("abc"));
    assertEquals("a__b_c_", reporter.filterCharacters("a..b.c."));
    assertEquals("a_b_c", reporter.filterCharacters("a\"b.c"));
  }

  /**
   * Tests that the registered metrics' names don't contain invalid characters.
   */
  @Test
  public void testAddingMetrics() throws Exception {
    Configuration configuration = new Configuration();
    String taskName = "test\"Ta\"..sk";
    String jobName = "testJ\"ob:-!ax..?";
    String hostname = "loc<>al\"::host\".:";
    String taskManagerId = "tas:kMana::ger";
    String counterName = "testCounter";

    configuration.setString(MetricOptions.SCOPE_NAMING_TASK, "<host>.<tm_id>.<job_name>");
    configuration.setString(MetricOptions.SCOPE_DELIMITER, "_");
    configuration.setString("metrics.reporters", "dwreporter");
    configuration
        .setString("metrics.reporter." + "dwreporter.class", DropwizardReporter.class.getName());

    MetricRegistryConfiguration metricRegistryConfiguration = MetricRegistryConfiguration
        .fromConfiguration(configuration);

    MetricRegistryImpl metricRegistry = new MetricRegistryImpl(
        metricRegistryConfiguration);

    char delimiter = metricRegistry.getDelimiter();

    TaskManagerMetricGroup tmMetricGroup = new TaskManagerMetricGroup(metricRegistry, hostname,
        taskManagerId);
    TaskManagerJobMetricGroup tmJobMetricGroup = new TaskManagerJobMetricGroup(metricRegistry,
        tmMetricGroup, new JobID(), jobName);
    TaskMetricGroup taskMetricGroup = new TaskMetricGroup(metricRegistry, tmJobMetricGroup,
        new JobVertexID(), new AbstractID(), taskName, 0, 0);

    SimpleCounter myCounter = new SimpleCounter();
    com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
    DropwizardMeterWrapper meterWrapper = new DropwizardMeterWrapper(dropwizardMeter);

    taskMetricGroup.counter(counterName, myCounter);
    taskMetricGroup.meter("meter", meterWrapper);

    List<MetricReporter> reporters = metricRegistry.getReporters();

    assertTrue(reporters.size() == 1);
    MetricReporter metricReporter = reporters.get(0);

    assertTrue(metricReporter instanceof DropwizardReporter, "Reporter should be of type DropwizardReporter");

    DropwizardReporter reporter = (DropwizardReporter) metricReporter;

    Map<Counter, String> counters = reporter.getCounters();
    assertTrue(counters.containsKey(myCounter));

    Map<Meter, String> meters = reporter.getMeters();
    assertTrue(meters.containsKey(meterWrapper));

    String expectedCounterName = reporter.filterCharacters(hostname)
        + delimiter
        + reporter.filterCharacters(taskManagerId)
        + delimiter
        + reporter.filterCharacters(jobName)
        + delimiter
        + reporter.filterCharacters(counterName);

    assertEquals(expectedCounterName, counters.get(myCounter));

    metricRegistry.shutdown().get();
  }

  /**
   * This test verifies that metrics are properly added and removed to/from the DropwizardReporter
   * and the underlying Dropwizard MetricRegistry.
   */
  @Test
  public void testMetricCleanup() {
    DropwizardReporter rep = new DropwizardReporter();
    rep.registry = new MetricRegistry();

    MetricGroup mp = new UnregisteredMetricsGroup();

    Counter c = new SimpleCounter();
    Meter m = new Meter() {
      @Override
      public void markEvent() {
      }

      @Override
      public void markEvent(long n) {
      }

      @Override
      public double getRate() {
        return 0;
      }

      @Override
      public long getCount() {
        return 0;
      }
    };
    Histogram h = new Histogram() {
      @Override
      public void update(long value) {

      }

      @Override
      public long getCount() {
        return 0;
      }

      @Override
      public HistogramStatistics getStatistics() {
        return null;
      }
    };
    Gauge g = new Gauge() {
      @Override
      public Object getValue() {
        return null;
      }
    };

    rep.notifyOfAddedMetric(c, "counter", mp);
    assertEquals(1, rep.getCounters().size());
    assertEquals(1, rep.registry.getCounters().size());

    rep.notifyOfAddedMetric(m, "meter", mp);
    assertEquals(1, rep.getMeters().size());
    assertEquals(1, rep.registry.getMeters().size());

    rep.notifyOfAddedMetric(h, "histogram", mp);
    assertEquals(1, rep.getHistograms().size());
    assertEquals(1, rep.registry.getHistograms().size());

    rep.notifyOfAddedMetric(g, "gauge", mp);
    assertEquals(1, rep.getGauges().size());
    assertEquals(1, rep.registry.getGauges().size());

    rep.notifyOfRemovedMetric(c, "counter", mp);
    assertEquals(0, rep.getCounters().size());
    assertEquals(0, rep.registry.getCounters().size());

    rep.notifyOfRemovedMetric(m, "meter", mp);
    assertEquals(0, rep.getMeters().size());
    assertEquals(0, rep.registry.getMeters().size());

    rep.notifyOfRemovedMetric(h, "histogram", mp);
    assertEquals(0, rep.getHistograms().size());
    assertEquals(0, rep.registry.getHistograms().size());

    rep.notifyOfRemovedMetric(g, "gauge", mp);
    assertEquals(0, rep.getGauges().size());
    assertEquals(0, rep.registry.getGauges().size());
  }

}
