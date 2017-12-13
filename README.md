Express way project with Flink
==============================

This implementation is using dataSet class.

to Compile
==========

mvn clean package -Pbuild-jar

to Run
======

flink run -c master2017.flink.VehicleTelematics target/flinkProgram-1.0.jar &lt;input_csv_file&gt; &lt;data_sink_folder&gt;

Parallelism
===========

Depending of the flink configuration file, the project could run using the flag `-p <number>`. Indicating the parallelism with which the program will run.