#!/usr/bin/env bash
sbt assembly
java -Xms1g -Xmx2g -classpath ./target/scala-2.11/datafusion-benchmarks-assembly-0.1.0.jar GenerateWKT