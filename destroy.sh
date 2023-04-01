#!/usr/bin/env bash

env | grep 'namespace\|db_host\|image'

function fun() {
    sui_network=$1

    #envsubst < deploy.yaml | kubectl --namespace $namespace -f - delete

    cat deploy.yaml | \
    sed 's/${namespace}/namespace/g' | sed "s/namespace/$namespace/g" | \
    sed 's/${sui_network}/sui_network/g' | sed "s/sui_network/$sui_network/g" | \
    sed 's/${image_sui_archive}/image_sui_archive/g' | sed "s@image_sui_archive@${image_sui_archive}@g" |\
    sed 's/${db_password_sui_archive}/db_password_sui_archive/g' | sed "s/db_password_sui_archive/$db_password_sui_archive/g" | \
    sed 's/${db_host}/db_host/g' | sed "s/db_host/$db_host/g" | \
    kubectl --namespace $namespace -f - delete
}

fun devnet
