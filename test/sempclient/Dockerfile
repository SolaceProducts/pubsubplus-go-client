# pubsubplus-go-client
#
# Copyright 2023-2025 Solace Corporation. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
