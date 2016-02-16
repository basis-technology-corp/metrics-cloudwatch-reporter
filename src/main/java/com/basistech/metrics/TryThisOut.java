/*
* Copyright 2016 Basis Technology Corp.
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
                .dimension("instanceId", instanceId)
                .namespace("some-namespace")
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
        synchronized (tto) {
            while (true) {
                tto.wait();
            }
        }
    }
}
