#!/usr/bin/env bash
sbt compile
java -Xms1g -Xmx4g -classpath ./target/scala-2.11/datafusion-benchmarks-assembly-0.1.0.jar GenerateWKT 100000000
