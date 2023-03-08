#!/usr/bin/env bash

source sui.env

psql -c "truncate table sui_devnet.event cascade"

for file in data/*csv; do
  echo $file;

  psql -c "\copy sui_devnet.event from '$file' csv header"

done
