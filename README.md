[![Build Status](https://travis-ci.org/GuillaumeArnaud/conflator.svg?branch=master)](https://travis-ci.org/GuillaumeArnaud/conflator)

conflator
=========

The main goal of this library is to test different implementations of conflation algorithm in java.

conflation
==========

Let's take an example for explaining what conflation is.

A set of price updates is received continuously by a financial system which is represented by a queue. Then theses
updates are processed and sent to dashboards or other analysis systems. The operator in the output doesn't care of the
historic of updates but wants to the last known price as fast as it's possible. When the first update of the product A
arrives, the system processes it and then takes the next update in the queue which concerns the product B, then
product C and so on... At least an other update for product A arrives. The system processing could be so long that  the
first update is not really interesting anymore. The conflation is the mean to shrink the queue in order to
anticipating the futur updates already in the queue and which could be merged to older updates or even replace them.


So the goal is when producer sends for instance (in chronological order):

    A1, B1, C1, A2, A3, B2, C2

The consumer will receive (f is any function that could merge or discard the given messages):

    f(A1,A2,A3), f(B1,B2), f(C1,C2)


Of course, in according to the throughput of producers and consumers, the conflation could be partial:

    f(A1), f(B1,B2), f(C1,C2), f(A2,A3)


A consequence of this process, is that the total number of messages, at an instant, should be minimized. Ideally, it
should not be greater that the number of different products.

api
===

As the goal is to test several implementations, two interfaces are exposed:

* Conflator: the engine allowing the conflation processing
* Message: the schema of each messages

See the javadocs of these classes for more details.


benchmark
=========

Of course the performance is one of the principal criteria for validating the implementations. Microbenchmarks are
implemented thanks to JMH:

    $ mvn package
    $ java -jar target/microbenchmarks.jar


