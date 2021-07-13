#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

END_TO_END_DIR="`dirname \"$0\"`" # relative
END_TO_END_DIR="`( cd \"$END_TO_END_DIR\" && pwd -P)`" # absolutized and normalized
if [ -z "$END_TO_END_DIR" ] ; then
    # error; for some reason, the path is not accessible
    # to the script (e.g. permissions re-evaled after suid)
    exit 1  # fail
fi

export END_TO_END_DIR

if [ -z "$FLINK_DIR" ] ; then
    echo "You have to export the Flink distribution directory as FLINK_DIR"
    exit 1
fi

if [ -z "$FLINK_LOG_DIR" ] ; then
    export FLINK_LOG_DIR="$FLINK_DIR/logs"
fi

# On Azure CI, use artifacts dir
if [ -z "$DEBUG_FILES_OUTPUT_DIR" ] ; then
    export DEBUG_FILES_OUTPUT_DIR="$FLINK_LOG_DIR"
fi

source "${END_TO_END_DIR}/../tools/ci/maven-utils.sh"
source "${END_TO_END_DIR}/test-scripts/test-runner-common.sh"

function run_on_exit {
    collect_coredumps $(pwd) $DEBUG_FILES_OUTPUT_DIR
    collect_dmesg $DEBUG_FILES_OUTPUT_DIR
}

on_exit run_on_exit

if [[ ${PROFILE} == *"enable-adaptive-scheduler"* ]]; then
	echo "Enabling adaptive scheduler properties"
	export JVM_ARGS="-Dflink.tests.enable-adaptive-scheduler=true"
fi

FLINK_DIR="`( cd \"$FLINK_DIR\" && pwd -P)`" # absolutized and normalized

echo "flink-end-to-end-test directory: $END_TO_END_DIR"
echo "Flink distribution directory: $FLINK_DIR"

echo "Java and Maven version"
java -version
run_mvn -version

echo "Free disk space"
df -h

echo "Running with profile '$PROFILE'"

# Template for adding a test:

# run_test "<description>" "$END_TO_END_DIR/test-scripts/<script_name>" ["skip_check_exceptions"]

# IMPORTANT:
# With the "skip_check_exceptions" flag one can disable default exceptions and errors checking in log files. This should be done
# carefully though. A valid reasons for doing so could be e.g killing TMs randomly as we cannot predict what exception could be thrown. Whenever
# those checks are disabled, one should take care that a proper checks are performed in the tests itself that ensure that the test finished
# in an expected state.

printf "\n\n==============================================================================\n"
printf "Running bash end-to-end tests\n"
printf "==============================================================================\n"


run_test "SQL Client end-to-end test" "$END_TO_END_DIR/test-scripts/test_sql_client.sh"

run_test "TPC-H end-to-end test" "$END_TO_END_DIR/test-scripts/test_tpch.sh"
run_test "TPC-DS end-to-end test" "$END_TO_END_DIR/test-scripts/test_tpcds.sh"

printf "\n[PASS] All bash e2e-tests passed\n"

printf "\n\n==============================================================================\n"
printf "Running Java end-to-end tests\n"
printf "==============================================================================\n"


LOG4J_PROPERTIES=${END_TO_END_DIR}/../tools/ci/log4j.properties

MVN_LOGGING_OPTIONS="-Dlog.dir=${DEBUG_FILES_OUTPUT_DIR} -DlogBackupDir=${DEBUG_FILES_OUTPUT_DIR} -Dlog4j.configurationFile=file://$LOG4J_PROPERTIES"
MVN_COMMON_OPTIONS="-Dflink.forkCount=2 -Dflink.forkCountTestPackage=2 -Dfast -Pskip-webui-build"
e2e_modules=$(find flink-end-to-end-tests -mindepth 2 -maxdepth 5 -name 'pom.xml' -printf '%h\n' | sort -u | tr '\n' ',')
e2e_modules="${e2e_modules},$(find flink-walkthroughs -mindepth 2 -maxdepth 2 -name 'pom.xml' -printf '%h\n' | sort -u | tr '\n' ',')"

PROFILE="$PROFILE -Pe2e-travis1 -Pe2e-travis2 -Pe2e-travis3 -Pe2e-travis4 -Pe2e-travis5 -Pe2e-travis6"
run_mvn ${MVN_COMMON_OPTIONS} ${MVN_LOGGING_OPTIONS} ${PROFILE} verify -pl ${e2e_modules} -DdistDir=$(readlink -e build-target) -Dcache-dir=$E2E_CACHE_FOLDER -Dcache-ttl=P3M -Dcache-download-attempt-timeout=4min -Dcache-download-global-timeout=10min

EXIT_CODE=$?


exit $EXIT_CODE
