#!/usr/bin/env bash

#curl -H 'Content-Type: application/json' \
#     -d '{"jsonrpc":"2.0","id":"0","method":"suix_queryEvents","params":[{"All":[]}]}' \
#     https://fullnode.devnet.sui.io

curl -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":"0","method":"suix_queryEvents","params":[{"All":[]}, {"txDigest": "2ZKQwCVhFHKm48CnVGteQMxSvTGnKbYm3Qi7rRPLgp4N", "eventSeq": "0"},1]}' \
     https://fullnode.devnet.sui.io