out: start run
	cat ~/flink-1.3.2/log/flink-dlilue-jobmanager-0-ataraxia.out

start: stop
	start-local.sh

stop:
	stop-local.sh

run: compile
	flink run -c master2017.flink.VehicleTelematics target/flinkProgram-1.0.jar ../traffic-3xways ../outputs

compile:
	mvn clean package -Pbuild-jar

