FROM apache/spark:3.5.1

USER root

COPY target/scala-2.12/compute_metrics.jar /opt/app.jar

WORKDIR /opt

ENTRYPOINT ["/opt/spark/bin/spark-submit", \
    "--master", "local[*]", \
    "--deploy-mode", "client", \
    "--class", "com.batchpipeline.metrics.Main", \
    "/opt/app.jar"]
