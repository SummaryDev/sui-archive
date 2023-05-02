-- db-default-privileges
alter default privileges for role sui_archive in schema sui_${sui_network}${sui_shard} grant select on tables to hasura;
alter default privileges for role sui_archive in schema sui_${sui_network}${sui_shard} grant select on tables to superset;
alter default privileges for role sui_archive in schema sui_${sui_network}${sui_shard} grant select on tables to metabase;
alter default privileges for role sui_archive in schema sui_${sui_network}${sui_shard} grant select on tables to redash_sui;

set search_path to sui_${sui_network}${sui_shard};

drop table if exists Event cascade;
create table Event
(
    txDigest          text,
    eventSeq          int,
    timestamp         timestamp,
    packageId         text,
    transactionModule text,
    sender            text,
    type              text,
    parsedJson        jsonb,
    bcs               text,
    primary key (txDigest, eventSeq)
);
create index on Event (txDigest);
create index on Event (timestamp);
create index on Event (packageId);
create index on Event (transactionModule);
create index on Event (sender);
create index on Event (type);

comment on table Event is 'SuiEvent';

comment on column Event.txDigest is 'Transaction digest';
comment on column Event.eventSeq is 'Event sequence';
comment on column Event.timestamp is 'Event time';
comment on column Event.packageId is 'Move package where this event was emitted';
comment on column Event.transactionModule is 'Move module where this event was emitted';
comment on column Event.sender is 'Sender Sui address';
comment on column Event.type is 'Move event type';
comment on column Event.parsedJson is 'Parsed json value of the event';
comment on column Event.bcs is 'Base 58 encoded bcs bytes of the move event';

-- comment on table TransferObjectEvent is E'@omit';
-- comment on table DeleteObjectEvent is E'@omit';
-- comment on table NewObjectEvent is E'@omit';
-- comment on table CoinBalanceChangeEvent is E'@omit';
-- comment on table MoveEvent is E'@omit';
-- comment on table MutateObjectEvent is E'@omit';
-- comment on table PublishEvent is E'@omit';