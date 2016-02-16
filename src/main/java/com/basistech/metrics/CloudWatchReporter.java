/*
* Copyright 2014 Basis Technology Corp.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.basistech.metrics;

import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.StatisticSet;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Metrics reporter for Amazon CloudWatch.
 * Use {@link CloudWatchReporter.Builder} to construct instances of this class.
 *
 * This reporter implements a simple mapping from Metrics to CloudWatch, and there is room for more
 * sophistication. The reporter applies a single set of dimensions to all of the metrics.
 *
 * The mappings are as follows:
 *<dl>
 * <dt>Gauge</dt>
 * <dd>If the value of the gauge is some subclass of `Number`, the value is reported. No unit is attached.</dd>
 * <dt>Counter</dt>
 * <dd>The value is reported. No unit is attached.</dd>
 * <dt>Histogram</dt>
 * <dd>The reporter prepares a `StatisticSet` with the values, min, max, and sum available from the reservoir.
 It does not report the percentiles, leaving it to CloudWatch to aggregate. This may not be very useful.</dd>
 * <dt>Timer</dt>
 * <dd>The reporter prepares a `StatisticSet` with the values, min, max, and sum available from the reservoir,
 * converted to Microseconds, since that's the smallest unit that CloudWatch knows about. This may not be very useful.
 * </dd>
 *</dl>
 * My suspicion is that the primary use of this is to use a gauge as an ASG trigger, letting CloudWatch do the
 * aggregation.
 */
public final class CloudWatchReporter extends ScheduledReporter {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchReporter.class);

    private final AmazonCloudWatchClient client;
    private final String namespace;
    private final Collection<Dimension> dimensions;

    private CloudWatchReporter(MetricRegistry registry,
                               AmazonCloudWatchClient client,
                               String namespace,
                               TimeUnit rateUnit,
                               TimeUnit durationUnit,
                               MetricFilter filter, Map<String, String> dimensions) {
        super(registry, "cloudwatch-reporter", filter, rateUnit, durationUnit);
        this.client = client;
        this.namespace = namespace;
        this.dimensions = new ArrayList<>();
        for (Map.Entry<String, String> me : dimensions.entrySet()) {
            this.dimensions.add(new Dimension().withName(me.getKey()).withValue(me.getValue()));
        }
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        Collection<MetricDatum> data = new ArrayList<>();
        for (Map.Entry<String, Gauge> meg : gauges.entrySet()) {
            if (meg.getValue().getValue() instanceof Number) {
                Number num = (Number)meg.getValue().getValue();
                double val = num.doubleValue();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("gauge {} val {}", meg.getKey(), val);
                }
                data.add(new MetricDatum().withMetricName(meg.getKey()).withValue(val).withDimensions(dimensions));
            }
        }
        for (Map.Entry<String, Counter> mec : counters.entrySet()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("counter {} val {}", mec.getKey(), mec.getValue().getCount());
            }
            data.add(new MetricDatum().withMetricName(mec.getKey()).withValue((double) mec.getValue().getCount())
                     .withDimensions(dimensions));
        }
        for (Map.Entry<String, Histogram> meh : histograms.entrySet()) {
            Snapshot snapshot = meh.getValue().getSnapshot();
            double sum = 0;
            for (double val : snapshot.getValues()) {
                sum += val;
            }
            StatisticSet stats = new StatisticSet().withMaximum((double) snapshot.getMax())
                    .withMinimum((double) snapshot.getMin())
                    .withSum(sum)
                    .withSampleCount((double) snapshot.getValues().length);
            if (LOG.isDebugEnabled()) {
                LOG.debug("histogram {}: {}", meh.getKey(), stats);
            }
            data.add(new MetricDatum().withMetricName(meh.getKey())
                    .withDimensions(dimensions)
                    .withStatisticValues(stats));
        }
        for (Map.Entry<String, Timer> met : timers.entrySet()) {
            Timer timer = met.getValue();
            Snapshot snapshot = timer.getSnapshot();
            double sum = 0;
            for (double val : snapshot.getValues()) {
                sum += val;
            }
            // Metrics works in Nanoseconds, which is not one of Amazon's favorites.
            double max = (double)TimeUnit.NANOSECONDS.toMicros(snapshot.getMax());
            double min = (double)TimeUnit.NANOSECONDS.toMicros(snapshot.getMin());
            double sumMicros = TimeUnit.NANOSECONDS.toMicros((long)sum);
            StatisticSet stats = new StatisticSet()
                    .withMaximum(max)
                    .withMinimum(min)
                    .withSum(sumMicros)
                    .withSampleCount((double) snapshot.getValues().length);
            if (LOG.isDebugEnabled()) {
                LOG.debug("timer {}: {}", met.getKey(), stats);
            }
            data.add(new MetricDatum().withMetricName(met.getKey())
                    .withDimensions(dimensions)
                    .withStatisticValues(stats)
                    .withUnit(StandardUnit.Microseconds));
        }
        PutMetricDataRequest put = new PutMetricDataRequest();
        put.setNamespace(namespace);
        put.setMetricData(data);
        try {
            client.putMetricData(put);
        } catch (Throwable t) {
            LOG.error("Failed to put metrics", t);
        }
    }

    /**
     * Builder class for the reporter.
     */
    public static class Builder {
        MetricRegistry registry;
        AmazonCloudWatchClient client;
        String namespace;
        TimeUnit rateUnit;
        TimeUnit durationUnit;
        MetricFilter filter;
        Map<String, String> dimensions;

        /**
         * Set up the builder.
         * @param registry which registry to report from.
         * @param client a client to use to push the metrics to CloudWatch.
         */
        public Builder(MetricRegistry registry, AmazonCloudWatchClient client) {
            this.registry = registry;
            this.client = client;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.dimensions = new HashMap<>();
        }

        /**
         * Set the namespace.
         * @param namespace the namespace.
         * @return this.
         */
        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        /**
         * Set the rate unit.
         * @param rateUnit the rate unit. The default is {@link TimeUnit#SECONDS}.
         * @return this
         */
        public Builder rateUnit(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Set the duration unit.
         * @param durationUnit the duration unit. The default is {@link TimeUnit#MILLISECONDS}.
         * @return this
         */
        public Builder durationUnit(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Set the filter.
         * @param filter The filter. Default is no filter; all metrics are reported.
         * @return this.
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Add a single dimension to the collection of dimensions for all the metrics
         * reported by this reporter.
         * @param key Dimension name.
         * @param value Dimension value.
         * @return this.
         */
        public Builder dimension(String key, String value) {
            this.dimensions.put(key, value);
            return this;
        }

        /**
         * Construct the reporter.
         * @return the reporter.
         */
        public CloudWatchReporter build() {
            return new CloudWatchReporter(registry, client, namespace, rateUnit,
                    durationUnit, filter, dimensions);
        }
    }
}
