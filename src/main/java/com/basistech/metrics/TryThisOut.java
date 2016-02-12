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
import com.amazonaws.util.EC2MetadataUtils;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Timer;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Trivial CLI just to see if this works.
 */
public class TryThisOut {
    private final MetricRegistry registry;
    private Gauge<Integer> gauge;
    private int gaugeValue;
    private Counter counter;
    private Histogram histogram;
    private Meter meter;
    private Timer timer;
    private CloudWatchReporter reporter;
    private ScheduledExecutorService executorService;

    public TryThisOut() {
        registry = new MetricRegistry();
        gauge = new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return gaugeValue;
            }
        };
        registry.register("gauge", gauge);
        counter = new Counter();
        registry.register("counter", counter);
        histogram = new Histogram(new SlidingTimeWindowReservoir(10, TimeUnit.SECONDS));
        registry.register("histogram", histogram);
        meter = new Meter();
        registry.register("meter", meter);
        timer = new Timer();
        registry.register("timer", timer);

        String instanceId = EC2MetadataUtils.getInstanceId();
        // try out the default constructor.
        AmazonCloudWatchClient client = new AmazonCloudWatchClient();
        reporter = new CloudWatchReporter.Builder(registry, client)
                .addDimensions("instanceId", instanceId)
                .setNamespace("some-namespace")
                .build();
        reporter.start(5, TimeUnit.SECONDS);

        executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(new Runnable() {
            private int counterVal;
            private Random random = new Random();

            @Override
            public void run() {
                gaugeValue = counterVal++;
                counter.inc();
                Timer.Context context = timer.time();
                meter.mark();
                histogram.update((int)(random.nextGaussian() * 10));
                context.stop();
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws Exception {
        TryThisOut tto = new TryThisOut();
        tto.wait();
    }
}
