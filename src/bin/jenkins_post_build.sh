#!/usr/bin/env bash
exec 2>&1
set -x
set -o errexit -o nounset -o pipefail

export NETFLIX_ENVIRONMENT=prod
PRESTO_VERSION=`ls presto-server/target/*.tar.gz  | xargs -n 1 basename | cut -d'-' -f3|cut -d'.' -f1,2`
CLI_BUILD_PATH=s3://netflix-bigdataplatform/presto/clients/${PRESTO_VERSION}/${BUILD_NUMBER_FORMATTED}/presto-cli
echo "Pushing presto-cli to $CLI_BUILD_PATH"
/apps/python/bin/python /usr/local/bin/assume-role -a arn:aws:iam::219382154434:role/BDP_JENKINS_ROLE aws s3 cp presto-cli/target/presto-cli*executable*.jar $CLI_BUILD_PATH

SERVER_BUILD_DIR=s3://netflix-bigdataplatform/presto/builds/${PRESTO_VERSION}/${BUILD_NUMBER_FORMATTED}/
echo "Pushing presto-server to $SERVER_BUILD_DIR"
/apps/python/bin/python /usr/local/bin/assume-role -a arn:aws:iam::219382154434:role/BDP_JENKINS_ROLE aws s3 cp presto-server/target/*tar.gz $SERVER_BUILD_DIR

if [[ "${STAGE_ON_S3:-false}" == "true" ]]; then
  CLI_DIR=s3://netflix-bigdataplatform/presto/clients/${PRESTO_VERSION}/presto-cli
  SERVER_DIR=s3://netflix-bigdataplatform/presto/builds/${PRESTO_VERSION}/
  echo "uploading artifacts to s3 release location $CLI_DIR and $SERVER_DIR."
  /apps/python/bin/python /usr/local/bin/assume-role -a arn:aws:iam::219382154434:role/BDP_JENKINS_ROLE aws s3 cp presto-cli/target/presto-cli*executable*.jar $CLI_DIR
  /apps/python/bin/python /usr/local/bin/assume-role -a arn:aws:iam::219382154434:role/BDP_JENKINS_ROLE aws s3 cp presto-server/target/*tar.gz $SERVER_DIR
else
  echo "Not uploading to s3 as STAGE_ON_S3 is not set to true."
fi
