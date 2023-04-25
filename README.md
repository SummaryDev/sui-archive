# Sui Archive

This is the archiver component for Sui Data Tool. It queries Sui full
node JSON-RPC API for events emitted by smart contracts and persist them
into a Postgres database. The data can then be queried with SQL via BI
tools, and with GraphQL from a web explorer or via http API.

## What is Sui Data Tool

Sui Data Tool is a platform that indexes Sui data and lets developers
and analysts explore it with SQL and GraphQL queries. The flexibility of
SQL allows to get answers to general stats and to niche questions. Query
results can be consumed via web interfaces or the API. Live charts and
dashboards can be built from the results, published and embedded into
other websites.

## Why

This is a platform tool that indexes all events and provides access to
them with the power and flexibility of SQL. Rather than having each team
develop their own indexer, one platform saves effort, helps discover
data from other projects and lets users share their findings. Access to
raw data is open to all, as well as to the results of research: queries
and dashboards are shared, helping spread the knowledge and build
community.
