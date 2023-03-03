# Based on https://github.com/swagger-api/swagger-codegen/blob/3.0.0/modules/swagger-codegen-cli/Dockerfile as general reference
# specifically using https://github.com/swagger-api/swagger-codegen/blob/v3.0.40/modules/swagger-codegen-cli/Dockerfile as reference
# use openjdk:8-jre-alpine as base as multi arch is supported for x86_64 and arm64
FROM openjdk:8-jre-alpine
#pull codegen jar from maven.org
ARG SWAGGER_VER=3.0.40
ENV SWAGGER_VERSION=${SWAGGER_VER}
ADD https://repo1.maven.org/maven2/io/swagger/codegen/v3/swagger-codegen-cli/${SWAGGER_VERSION}/swagger-codegen-cli-${SWAGGER_VERSION}.jar /opt/swagger-codegen-cli/swagger-codegen-cli.jar
# use entrypoint and cmd from reference dockerfile
ENTRYPOINT ["java", "-jar", "/opt/swagger-codegen-cli/swagger-codegen-cli.jar"]

CMD ["help"]
