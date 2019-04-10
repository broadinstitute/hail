#!/bin/bash

set -ex

. activate hail-pipeline

PYTEST_ARGS=${PYTEST_ARGS:- -v --failed-first}

cleanup() {
    set +e
    trap "" INT TERM

    for table in ${tables[@]}; do
	python3 -c "from batch.database import Database; db = Database.create_synchronous(\"$CLOUD_SQL_CONFIG_PATH\"); db.drop_table_sync(\"$table\")"
    done    
    
    [[ -z $server_pid ]] || (kill $server_pid; sleep 2; kill -9 $server_pid)
    [[ -z $proxy_pid ]] || (kill $proxy_pid; sleep 2; kill -9 $proxy_pid)
}
trap cleanup EXIT
trap "exit 24" INT TERM

if [[ -z $IN_HAIL_CI ]]; then
    export CLOUD_SQL_CONFIG_PATH=`pwd`/batch-secrets/batch-test-cloud-sql-config.json
    connection_name=$(jq -r '.connection_name' $CLOUD_SQL_CONFIG_PATH)
    host=$(jq -r '.host' $CLOUD_SQL_CONFIG_PATH)
    port=$(jq -r '.port' $CLOUD_SQL_CONFIG_PATH)
    ./cloud_sql_proxy -instances=$connection_name=tcp:$port &
    proxy_pid=$!
    ../until-with-fuel 30 curl -fL $host:$port
else
    export CLOUD_SQL_CONFIG_PATH=/batch-secrets/batch-test-cloud-sql-config.json
fi

export JOBS_TABLE=jobs-$(../generate-uid.sh)
export JOBS_PARENTS_TABLE=jobs-parents-$(../generate-uid.sh)
export BATCH_TABLE=batch-$(../generate-uid.sh)
export BATCH_JOBS_TABLE=batch-jobs-$(../generate-uid.sh)
tables=($JOBS_TABLE $JOBS_PARENTS_TABLE $BATCH_TABLE $BATCH_JOBS_TABLE)

cd ../batch/
python -c 'import batch.server; batch.server.serve()' &
server_pid=$!
cd ../pipeline/

: $((tries = 0))
until curl -fL 127.0.0.1:5000/jobs >/dev/null 2>&1
do
    : $((tries = tries + 1)) && [ $tries -lt 30 ]
    sleep 1
done

BATCH_URL='http://127.0.0.1:5000' pytest ${PYTEST_ARGS} test
