#!/bin/sh

SCRIPT_DIR=$( cd "$( dirname "$0" )" && pwd )
DYNAMO_LOCAL_TARBALL="http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.tar.gz"
DYNAMO_DESTINATION="$SCRIPT_DIR/dynamodb_local_latest"

echo $DYNAMO_DESTINATION
if [ ! -d "$DYNAMO_DESTINATION" ]; then
  mkdir $DYNAMO_DESTINATION
  curl -L $DYNAMO_LOCAL_TARBALL | tar xz -C "$DYNAMO_DESTINATION/"
fi

DYNAMO_LIB_PATH="$DYNAMO_DESTINATION/DynamoDBLocal_lib"
DYNAMO_JAR_PATH="$DYNAMO_DESTINATION/DynamoDBLocal.jar"

java -Djava.library.path=$DYNAMO_LIB_PATH -jar $DYNAMO_JAR_PATH -sharedDb
