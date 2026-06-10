#!/bin/bash
# Build a new zip file for the connector and upload it to s3.
# - base it on the original 3.3.0 zip file in s3
# - this approach is not ideal and only used for POC.
#
# Usage: 
# 	./buildMultiSchemaJar.sh clean      # clean build
# 	./buildMultiSchemaJar.sh 	    # incremental build


set -e # fail on error

CLEAN=${1:-""}

MULTI_SCHEMA_ZIP=snowflake-kafka-connector-multi-schema-3.3.0.zip
ORIGINAL_ZIP=custom-snowflakeinc-snowflake-kafka-connector-3.3.0.zip
S3_FOLDER=s3://sdx-051826739313-us-east-1-development/msk-connect-plugins

mvn -o $CLEAN install -DskipTests -Dgpg.skip=true

if [ ! -f "target/$ORIGINAL_ZIP" ]; then
	# download original file
	echo "Downloading original zip $ORIGINAL_ZIP "
	aws s3 cp $S3_FOLDER/$ORIGINAL_ZIP target/.
fi

rm -f target/$MULTI_SCHEMA_ZIP
cp target/$ORIGINAL_ZIP target/$MULTI_SCHEMA_ZIP

# replace the original jar fle inside the zip with the new jar file
#  - the path to the jar must match the path used inside the jar file
cd target

ZIP_FOLDER=snowflakeinc-snowflake-kafka-connector-3.3.0
rm -fr $ZIP_FOLDER 
mkdir -p $ZIP_FOLDER/lib

addJar() {
  JAR=$1
  JAR_FILE=$(basename $JAR)
  LIB_JAR=$ZIP_FOLDER/lib/$JAR_FILE
  echo "Updaating $LIB_JAR inside $MULTI_SCHEMA_ZIP"
  cp $JAR $ZIP_FOLDER/lib/.
  zip -u $MULTI_SCHEMA_ZIP $LIB_JAR
}
addJar snowflake-kafka-connector-3.3.0.jar
addJar ~/.m2/repository/io/nats/jnats/2.25.2/jnats-2.25.2.jar
addJar ~/.m2/repository/org/jspecify/jspecify/1.0.0/jspecify-1.0.0.jar

echo "Uploading $MULTI_SCHEMA_ZIP to s3 bucket $S3_FOLDER"
aws s3 cp $MULTI_SCHEMA_ZIP $S3_FOLDER/$MULTI_SCHEMA_ZIP --sse AES256
echo "Upload successful"
