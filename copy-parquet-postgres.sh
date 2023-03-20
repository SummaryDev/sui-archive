#!/bin/bash

# run with
# ssh2 "bash -s" < ./copy-parquet-postgres.sh 2023-03-07
# ssh3 "bash -s" < ./copy-parquet-postgres.sh 2023-03-08
# ssh2 "bash -s" < ./copy-parquet-postgres.sh 2023-03-09

#sudo yum install postgresql unzip -y && \
#wget --quiet https://github.com/duckdb/duckdb/releases/download/v0.7.1/duckdb_cli-linux-amd64.zip && \
#unzip -u duckdb_cli-linux-amd64.zip && \
#rm duckdb_cli-linux-amd64.zip

#export PGHOST=
#export PGDATABASE=
#export PGUSER=
#export PGPASSWORD=

# "2023-03-07"
d=$1

for n in "transferObject" "publish" "coinBalanceChange" "moveEvent" "mutateObject" "deleteObject" "newObject"; do
  e="${n}Event"
  if [ $n = "moveEvent" ]
    then e=$n
  fi

  echo ${n} ${e};

  ./duckdb -c "copy (select * from read_parquet('${n}-${d}.parquet')) to '${n}-${d}.csv' with (header 1)"

  psql -c "create table sui_devnet.tmp_table as select * from sui_devnet.${e} with no data"
  psql -c "\copy sui_devnet.tmp_table from '${n}-${d}.csv' csv header"
  psql -c "insert into sui_devnet.${e} select * from sui_devnet.tmp_table on conflict do nothing"
  psql -c "drop table sui_devnet.tmp_table"

done
