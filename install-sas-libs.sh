#!/bin/bash

export M2_REPO=${M2_REPO:-"${HOME}/.m2/repository"}

run_mvn_install() {
    local artifactId="${1}"
    local version="${2}"
    local file="${3}"
    local groupId=${4}

    local output=$(mktemp)

    echo -n "Installing ${file} as ${artifactId}... "
    mvn -U install:install-file -DgroupId="${groupId}"\
                          -DartifactId="${artifactId}"\
                          -Dversion="${version}" \
                          -Dpackaging=jar \
                          -Dfile="${file}" #2>> "${output}" >> "${output}"
    if [ $? -ne 0 ]; then
      echo "Failed. Checkout output for root cause: ${output}."
      exit 1
    fi
    rm -rf "${output}"
    echo 'Done.'
}

mvn_install_file() {
    local artifactId="${1}"
    local version="${2}"
    local file="${3}"
    local groupId=${4:-'org.jboss.seam'}

    local groupId_folder=$(echo ${groupId} | sed -e 's;\.;/;g')
    local path_to_jar="${M2_REPO}/${groupId_folder}/${artifactId}/${version}/${artifactId}-${version}.jar"

    if [ ! -e ${path_to_jar} ]; then
      run_mvn_install "${artifactId}" "${version}" "${file}" "${groupId}"
    else
      echo "File ${path_to_jar} already exists."
    fi
    rm -rf "${output}"
}

set -e

readonly SAS_VERSION='903000.2.0.20110601190000_v930'
readonly SAS_GROUP_ID='com.sas'

echo "Installing SAS into local repo (${M2_REPO}):"
mvn_install_file 'sas-core' "${SAS_VERSION}" './libs/sas.core.jar' "${SAS_GROUP_ID}"
mvn_install_file 'sas-intrnet-javatools' "${SAS_VERSION}" './libs/sas.intrnet.javatools.jar' "${SAS_GROUP_ID}"
mvn_install_file 'sas-svc-connection' "${SAS_VERSION}" './libs/sas.svc.connection.jar' "${SAS_GROUP_ID}"



