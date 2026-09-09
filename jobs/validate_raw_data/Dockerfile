FROM apache/spark:3.5.1

USER root

COPY target/scala-2.12/validate_raw_data.jar /opt/app.jar
COPY config /config

WORKDIR /opt

ENTRYPOINT ["/opt/spark/bin/spark-submit", \
    "--master", "local[*]", \
    "--deploy-mode", "client", \
    "--class", "com.batchpipeline.validate.Main", \
    "/opt/app.jar"]
