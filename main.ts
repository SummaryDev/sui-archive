import {JsonRpcProvider, devnetConnection, EventQuery, PaginatedEvents} from '@mysten/sui.js'
// import postgres from 'postgres'
import * as fs from 'fs'
import {stringify} from 'csv'

const folder = 'data'

function log(o: any) {
  console.log(o)
}

function pretty(o: any) {
  console.log(JSON.stringify(o, null, 2))
}

// connect to db
// const sql = postgres({ /* options */ }) // will use psql environment variables

function processEvents(res: PaginatedEvents) {
  // for(let event of res.data) {
  //   log(event)
  // }

  const values = res.data.map(o => {
    const name = Object.keys(o.event)[0]
    const data = Object.values(o.event)[0]
    const timestamp = new Date(o.timestamp).toISOString()

    return {
      txDigest: o.txDigest,
      eventSeq: o.id.eventSeq,
      name: name,
      data: data,
      timestamp: timestamp
    }
  })

  log(res.nextCursor)

  if(res.nextCursor)
    stringify(values, {header: true}).pipe(fs.createWriteStream(`${folder}/events-${res.nextCursor.txDigest}-${res.nextCursor.eventSeq}.csv`))

  return res.nextCursor
}

function main() {
  (async () => {

    // connect to Devnet
    const provider = new JsonRpcProvider(devnetConnection)

    const query: EventQuery = 'All'
    let cursor = null
    const limit = null
    const order = 'ascending'

    cursor = processEvents(await provider.getEvents(query, cursor, limit, order))

    while (cursor !== null) {
      cursor = processEvents(await provider.getEvents(query, cursor, limit, order))
    }

  })()
}

main()

