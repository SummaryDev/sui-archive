#!/usr/bin/env bash

env | grep 'namespace\|db_host\|image'

function fun() {
    sui_network=$1
    sui_shard=$2

    kubectl --namespace ${namespace} delete deployment sui-archive-${sui_network}${sui_shard}
}

#fun devnet

#fun testnet

fun mainnet
