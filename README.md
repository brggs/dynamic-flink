# Dynamic Flink

Dynamic Flink is a rules based streams processing application.  It reads a stream of events, and looks for matches against a list of rules.  These rules can be updated on the fly, without recompiling the code or stopping the application.

A variety of rules are supported, including matching specific contained within the data, exceeding thresholds or changes in the average number of certain events, as well as matching patterns of event sequences.

The job runs in the [Apache Flink](https://flink.apache.org/) streams processing framework.

While the application is fully functional, it's primarily shared as an example of how to implement this sort of system in Flink, specifically how to make use of the "Broadcast State Pattern".

For more information, please see this [series of blog posts](https://brggs.co.uk/dynamic-streams-processing-with-apache-flink/).

## Demo Environment

The quickest and easiest way to get the Dynamic Flink job up and running is to use the docker compose environment.  See [this page](demo-environment/README.md) for instructions.

## Building

Build dependencies:
* Java 1.8
* Maven
  
To build the job:

1. `cd flink-job`
2. `mvn package`

## Issues

If you have any problems, or have any feature requests, please add them here:

## License & Contribution

Dynamic Flink is licensed under the MIT License.  All contributions welcome.
