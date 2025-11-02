
# Use the standard Java 17 base image for Flex Templates
FROM gcr.io/dataflow-templates-base/java17-template-launcher-base

ARG WORKDIR=/dataflow/template
WORKDIR ${WORKDIR}

# Copy the fat JAR
COPY target/trips-0.0.1-SNAPSHOT.jar ${WORKDIR}

# Set the entry point to the launcher and main class
ENV FLEX_TEMPLATE_JAVA_MAIN_CLASS="org.pipeline.trips.TripsBatchMongoToGcs_scheduled"
ENV FLEX_TEMPLATE_JAVA_CLASSPATH="${WORKDIR}/trips-0.0.1-SNAPSHOT.jar"
