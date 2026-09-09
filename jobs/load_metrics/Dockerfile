FROM apache/spark:3.5.1

USER root

COPY target/scala-2.12/load_metrics.jar /opt/app.jar

WORKDIR /opt

ENTRYPOINT ["/opt/spark/bin/spark-submit", \
    "--master", "local[*]", \
    "--deploy-mode", "client", \
    "--class", "com.batchpipeline.load.Main", \
    "/opt/app.jar"]
