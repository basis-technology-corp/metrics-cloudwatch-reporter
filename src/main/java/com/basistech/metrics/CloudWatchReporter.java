/******************************************************************************
 * * This data and information is proprietary to, and a valuable trade secret
 * * of, Basis Technology Corp.  It is given in confidence by Basis Technology
 * * and may only be used as permitted under the license agreement under which
 * * it has been distributed, and in no other way.
 * *
 * * Copyright (c) 2015 Basis Technology Corporation All rights reserved.
 * *
 * * The technical data and information provided herein are provided with
 * * `limited rights', and the computer software provided herein is provided
 * * with `restricted rights' as those terms are defined in DAR and ASPR
 * * 7-104.9(a).
 ******************************************************************************/

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class CloudWatchReporter extends ScheduledReporter {

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
                double val = ((Number)meg.getValue()).doubleValue();
                data.add(new MetricDatum().withMetricName(meg.getKey()).withValue(val).withDimensions(dimensions));
            }
        }
        for (Map.Entry<String, Counter> mec : counters.entrySet()) {
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
            data.add(new MetricDatum().withMetricName(met.getKey())
                    .withDimensions(dimensions)
                    .withStatisticValues(stats));

        }
        PutMetricDataRequest put = new PutMetricDataRequest();
        put.setNamespace(namespace);
        put.setMetricData(data);
        client.putMetricData(put);
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
