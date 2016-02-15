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
 *
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
                LOG.debug(String.format("gauge %s val %f", meg.getKey(), val));
                data.add(new MetricDatum().withMetricName(meg.getKey()).withValue(val).withDimensions(dimensions));
            }
        }
        for (Map.Entry<String, Counter> mec : counters.entrySet()) {
            LOG.debug(String.format("counter %s val %d", mec.getKey(), mec.getValue().getCount()));
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
            LOG.debug(String.format("histogram %s: %s", meh.getKey(), stats));
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
            StatisticSet stats = new StatisticSet().withMaximum((double) snapshot.getMax())
                    .withMinimum((double) snapshot.getMin())
                    .withSum(sum)
                    .withSampleCount((double) snapshot.getValues().length);
            LOG.debug(String.format("timer %s %s", met.getKey(), stats));
            data.add(new MetricDatum().withMetricName(met.getKey())
                    .withDimensions(dimensions)
                    .withStatisticValues(stats));

        }
        PutMetricDataRequest put = new PutMetricDataRequest();
        put.setNamespace(namespace);
        put.setMetricData(data);
        LOG.debug("About to put some metrics");
        try {
            client.putMetricData(put);
        } catch (Throwable t) {
            LOG.error("Failed to put metrics", t);
        }
    }

    public static class Builder {
        MetricRegistry registry;
        AmazonCloudWatchClient client;
        String namespace;
        TimeUnit rateUnit;
        TimeUnit durationUnit;
        MetricFilter filter;
        Map<String, String> dimensions;

        public Builder(MetricRegistry registry, AmazonCloudWatchClient client) {
            this.registry = registry;
            this.client = client;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.dimensions = new HashMap<>();
        }

        public Builder setNamespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder setRateUnit(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public Builder setDurationUnit(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public Builder setFilter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public Builder addDimensions(String key, String value) {
            this.dimensions.put(key, value);
            return this;
        }

        public CloudWatchReporter build() {
            return new CloudWatchReporter(registry, client, namespace, rateUnit,
                    durationUnit, filter, dimensions);
        }
    }
}
