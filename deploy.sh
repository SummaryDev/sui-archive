#!/usr/bin/env bash

env | grep 'namespace\|sui_network\|db_host\|image'

function fun {
    sui_network=$1
    sui_shard=$2
    sui_cursor=$3

    ./db-create.sh

    cat deploy.yaml | \
    sed 's/${namespace}/namespace/g' | sed "s/namespace/$namespace/g" | \
    sed 's/${sui_network}/sui_network/g' | sed "s/sui_network/$sui_network/g" | \
    sed 's/${sui_shard}/sui_shard/g' | sed "s/sui_shard/$sui_shard/g" | \
    sed 's/${sui_cursor}/sui_cursor/g' | sed "s/sui_cursor/$sui_cursor/g" | \
    sed 's/${image_sui_archive}/image_sui_archive/g' | sed "s@image_sui_archive@${image_sui_archive}@g" |\
    sed 's/${db_password_sui_archive}/db_password_sui_archive/g' | sed "s/db_password_sui_archive/$db_password_sui_archive/g" | \
    sed 's/${db_host}/db_host/g' | sed "s/db_host/$db_host/g" | \
    kubectl --namespace ${namespace} -f - apply

#    --dry-run=client
}

#fun devnet

#fun testnet

#fun testnet 20230502 EDbXxHb6G2t2743hKTvvGuq5GQbGBjJWY89izmaaecm2

fun mainnet
