#!/usr/bin/env bash
exec 2>&1
set -x
set -o errexit -o nounset -o pipefail

export NETFLIX_ENVIRONMENT=prod

SERVER_BUILD_DIR=s3://netflix-bigdataplatform/presto/builds/341-SNAPSHOT/${BUILD_NUMBER_FORMATTED}/
CLIENT_BUILD_DIR=s3://netflix-bigdataplatform/presto/clients/341/presto-cli
echo "Pushing presto-server to $SERVER_BUILD_DIR"
echo "Pushing presto-cli executable to" $CLIENT_BUILD_DIR
/apps/python/bin/python /usr/local/bin/assume-role -a arn:aws:iam::219382154434:role/BDP_JENKINS_ROLE -r us-east-1 aws s3 cp presto-server/target/*tar.gz $SERVER_BUILD_DIR
/apps/python/bin/python /usr/local/bin/assume-role -a arn:aws:iam::219382154434:role/BDP_JENKINS_ROLE -r us-east-1 aws s3 cp presto-cli/target/presto-cli-341-SNAPSHOT-executable.jar $CLIENT_BUILD_DIR
