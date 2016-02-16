This is a reporter for the Dropwizard Metrics library. It delivers the information to Amazon CloudWatch.

This reporter implements a simple mapping from Metrics to CloudWatch, and there is room for more 
sophistication. The reporter applies a single set of dimensions to all of the metrics. 

The mappings are as follows:

* Gauge: If the value of the gauge is some subclass of `Number`, the value is reported. No unit is attached.
* Counter: The value is reported. No unit is attached.
* Histogram: The reporter prepares a `StatisticSet` with the values, min, max, and sum available from the reservoir. 
  It does not report the percentiles, leaving it to CloudWatch to aggregate. This may not be very useful.
* Timer: The reporter prepares a `StatisticSet` with the values, min, max, and sum available from the reservoir,
  converted to Microseconds, since that's the smallest unit that CloudWatch knows about. This may not be very useful.
  
My suspicion is that the primary use of this is to use a gauge as an ASG trigger, letting CloudWatch do the 
aggregation.

