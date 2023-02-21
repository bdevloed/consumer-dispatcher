export const BATCH_SIZE = process.env.BATCH_SIZE || 1000
export const SLEEP_BETWEEN_BATCHES = process.env.SLEEP_BETWEEN_BATCHES || 1000
export const MAX_ATTEMPTS = process.env.MAX_ATTEMPTS || 5
export const SLEEP_TIME_ON_FAIL = process.env.SLEEP_TIME_ON_FAIL || 1000

export const INGEST_GRAPH = process.env.INGEST_GRAPH || 'http://mu.semte.ch/graphs/ingest'

export const INITIAL_DISPATCH_ENDPOINT = process.env.DIRECT_DATABASE_ENDPOINT || process.env.MU_SPARQL_ENDPOINT
export const DELTA_NOTIFICATION_ENDPOINT = process.env.DELTA_NOTIFICATION || 'http://deltanotifier/'