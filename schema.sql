-- db-default-privileges
alter default privileges for role sui in schema sui_devnet grant select on tables to hasura;
alter default privileges for role sui in schema sui_devnet grant select on tables to superset;
alter default privileges for role sui in schema sui_devnet grant select on tables to metabase;
alter default privileges for role sui in schema sui_devnet grant select on tables to redash;

set search_path to sui_devnet;

-- drop table if exists event cascade;
-- txDigest,eventSeq,name,data,timestamp
-- create table event (txDigest text, eventSeq int, name text, data jsonb, timestamp timestamp, primary key(txDigest, eventSeq));
-- create index on event (txDigest);
-- create index on event (name);
-- create index on event (timestamp);

drop table if exists DeleteObjectEvent cascade;
create table DeleteObjectEvent
(
    txDigest          text,
    eventSeq          int,
    timestamp         timestamp,
    packageId         text,
    transactionModule text,
    sender            text,
    objectId          text,
    version           int,
    primary key (txDigest, eventSeq)
);
create index on DeleteObjectEvent (txDigest);
create index on DeleteObjectEvent (timestamp);
create index on DeleteObjectEvent (packageId);
create index on DeleteObjectEvent (transactionModule);
create index on DeleteObjectEvent (sender);
create index on DeleteObjectEvent (objectId);

drop table if exists TransferObjectEvent cascade;
create table TransferObjectEvent
(
    txDigest          text,
    eventSeq          int,
    timestamp         timestamp,
    packageId         text,
    transactionModule text,
    sender            text,
    recipient         text,
    objectType        text,
    objectId          text,
    version           int,
    primary key (txDigest, eventSeq)
);
create index on TransferObjectEvent (txDigest);
create index on TransferObjectEvent (timestamp);
create index on TransferObjectEvent (packageId);
create index on TransferObjectEvent (transactionModule);
create index on TransferObjectEvent (sender);
create index on TransferObjectEvent (recipient);
create index on TransferObjectEvent (objectType);
create index on TransferObjectEvent (objectId);

drop table if exists PublishEvent cascade;
create table PublishEvent
(
    txDigest  text,
    eventSeq  int,
    timestamp timestamp,
    sender    text,
    packageId text,
    version   int,
    digest    text,
    primary key (txDigest, eventSeq)
);
create index on PublishEvent (txDigest);
create index on PublishEvent (timestamp);
create index on PublishEvent (sender);
create index on PublishEvent (packageId);

drop table if exists CoinBalanceChangeEvent cascade;
create table CoinBalanceChangeEvent
(
    txDigest          text,
    eventSeq          int,
    timestamp         timestamp,
    packageId         text,
    transactionModule text,
    sender            text,
    owner             text,
    changeType        text,
    coinType          text,
    coinObjectId      text,
    version           int,
    amount            numeric,
    primary key (txDigest, eventSeq)
);
create index on CoinBalanceChangeEvent (txDigest);
create index on CoinBalanceChangeEvent (timestamp);
create index on CoinBalanceChangeEvent (packageId);
create index on CoinBalanceChangeEvent (transactionModule);
create index on CoinBalanceChangeEvent (sender);
create index on CoinBalanceChangeEvent (owner);
create index on CoinBalanceChangeEvent (changeType);
create index on CoinBalanceChangeEvent (coinType);
create index on CoinBalanceChangeEvent (coinObjectId);

drop table if exists MoveEvent cascade;
create table MoveEvent
(
    txDigest          text,
    eventSeq          int,
    timestamp         timestamp,
    packageId         text,
    transactionModule text,
    sender            text,
    type              text,
    fields            text,
    bcs               text,
    primary key (txDigest, eventSeq)
);
create index on MoveEvent (txDigest);
create index on MoveEvent (timestamp);
create index on MoveEvent (packageId);
create index on MoveEvent (transactionModule);
create index on MoveEvent (sender);
create index on MoveEvent (type);

drop table if exists MutateObjectEvent cascade;
create table MutateObjectEvent
(
    txDigest          text,
    eventSeq          int,
    timestamp         timestamp,
    packageId         text,
    transactionModule text,
    sender            text,
    objectType        text,
    objectId          text,
    version           int,
    primary key (txDigest, eventSeq)
);
create index on MutateObjectEvent (txDigest);
create index on MutateObjectEvent (timestamp);
create index on MutateObjectEvent (packageId);
create index on MutateObjectEvent (transactionModule);
create index on MutateObjectEvent (sender);
create index on MutateObjectEvent (objectType);
create index on MutateObjectEvent (objectId);

drop table if exists NewObjectEvent cascade;
create table NewObjectEvent
(
    txDigest          text,
    eventSeq          int,
    timestamp         timestamp,
    packageId         text,
    transactionModule text,
    sender            text,
    recipient         text,
    objectType        text,
    objectId          text,
    version           int,
    primary key (txDigest, eventSeq)
);
create index on NewObjectEvent (txDigest);
create index on NewObjectEvent (timestamp);
create index on NewObjectEvent (packageId);
create index on NewObjectEvent (transactionModule);
create index on NewObjectEvent (sender);
create index on NewObjectEvent (recipient);
create index on NewObjectEvent (objectType);
create index on NewObjectEvent (objectId);



alter table NewObjectEvent alter column recipient type jsonb using to_jsonb(recipient);

alter table TransferObjectEvent alter column recipient type jsonb using to_jsonb(recipient);
alter table CoinBalanceChangeEvent alter column owner type jsonb using to_jsonb(owner);
alter table MoveEvent alter column fields type jsonb using to_jsonb(fields);

-- {"AddressOwner": "0xf5b13d1484470ac6bce405b56cae9da216b83e08"}
-- select to_jsonb('{"AddressOwner": "0xf5b13d1484470ac6bce405b56cae9da216b83e08"}');
-- select json_build_object('AddressOwner', '0xf5b13d1484470ac6bce405b56cae9da216b83e08');