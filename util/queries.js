import { sparqlEscapeUri, sparqlEscapeString, sparqlEscapeDateTime, uuid } from "mu";
import { query, update } from "mu";

import {
  BATCH_SIZE,
  SLEEP_BETWEEN_BATCHES,
  MAX_ATTEMPTS,
  SLEEP_TIME_ON_FAIL
} from "../config";

import { PREFIXES } from "../constants";

const CREATOR = 'http://lblod.data.gift/services/loket-consumer-dispatcher-service';


export async function insertIntoGraph(graph, triples) {
  data = [... new Set(triples)];
  if (data.length) {
    batchedDbUpdate(
      graph,
      data,
    )
  } else {
    console.log("No data to insert");
  }
}

export const update_template = (graph, data) => `
INSERT DATA {
  GRAPH <${graph}> {
    ${data.join('\n    ')}
  }
}`

export const subject_type_query = (graph, subjects) => `
SELECT ?subject ?type
FROM <${graph}>
WHERE {
  VALUES ?subject {
    ${subjects.map(uri => `<${uri}>`).join('\n    ')}
  }
  ?subject a ?type
}`

// TODO: configurable initiall sync uris, handle failed initial syncs
export const initial_syncs_done_query = (operation) => `
${PREFIXES}
SELECT DISTINCT ?s ?created WHERE {
  VALUES ?operation {
    ${sparqlEscapeUri(operation)}
  }
  ?s a <http://vocab.deri.ie/cogs#Job> ;
    task:operation ?operation ;
    adms:status <http://redpencil.data.gift/id/concept/JobStatus/success> ;
    dct:created ?created.

}
ORDER BY DESC(?created)
LIMIT 1`

async function batchedDbUpdate(
  graph,
  triples
) {

  for (let i = 0; i < triples.length; i += BATCH_SIZE) {
    console.log(`Inserting triples in batch: ${i}-${i + BATCH_SIZE}`);

    const batch = triples.slice(i, i + BATCH_SIZE).join('\n');

    const insertCall = async () => {
      await update(update_template(graph, batch));
    };

    await operationWithRetry(insertCall, 0);

    console.log(`Sleeping before next query execution: ${SLEEP_BETWEEN_BATCHES}`);
    await new Promise(r => setTimeout(r, SLEEP_BETWEEN_BATCHES));
  }
}

async function operationWithRetry(callback,
  attempt) {
  try {
    if (typeof callback === "function")
      return await callback();
    else // Catch error from promise - not how I would do it normally, but allows re use of existing code.
      return await callback;
  }
  catch (e) {
    console.log(`Operation failed for ${callback.toString()}, attempt: ${attempt} of ${MAX_ATTEMPTS}`);
    console.log(`Error: ${e}`);
    console.log(`Sleeping ${SLEEP_TIME_ON_FAIL} ms`);

    if (attempt >= MAX_ATTEMPTS) {
      console.log(`Max attempts reached for ${callback.toString()}, giving up`);
      throw e;
    }

    await new Promise(r => setTimeout(r, SLEEP_TIME_ON_FAIL));
    return operationWithRetry(callback, ++attempt);
  }
}

export async function all_initial_syncs_done() {
  try {
    for (const operation of [
      'http://redpencil.data.gift/id/jobs/concept/JobOperation/deltas/consumer/initialSync/leidinggevenden',
      'http://redpencil.data.gift/id/jobs/concept/JobOperation/deltas/consumer/initialSync/mandatarissen'
    ]) {
      const result = await query(initial_syncs_done_query(operation));
      const initial_sync_done = !!(result && result.results.bindings.length);
      if (!initial_sync_done) {
        return false;
      }
    }
    return true;
  } catch (e) {
    const error_message = `Error while checking if initial syncs are done: ${e.message ? e.message : e}`;
    console.log(error_message);
    sendErrorAlert({
      message: error_message
    });
    return false;
  }
}


export async function sendErrorAlert({ message, detail, reference }) {
  if (!message)
    throw 'Error needs a message describing what went wrong.';
  const id = uuid();
  const uri = `http://data.lblod.info/errors/${id}`;
  const q = `
      PREFIX mu:   <http://mu.semte.ch/vocabularies/core/>
      PREFIX oslc: <http://open-services.net/ns/core#>
      PREFIX dct:  <http://purl.org/dc/terms/>
      INSERT DATA {
        GRAPH <http://mu.semte.ch/graphs/error> {
            ${sparqlEscapeUri(uri)} a oslc:Error ;
                    mu:uuid ${sparqlEscapeString(id)} ;
                    dct:subject ${sparqlEscapeString('Dispatch worship positions')} ;
                    oslc:message ${sparqlEscapeString(message)} ;
                    dct:created ${sparqlEscapeDateTime(new Date().toISOString())} ;
                    dct:creator ${sparqlEscapeUri(CREATOR)} .
            ${reference ? `${sparqlEscapeUri(uri)} dct:references ${sparqlEscapeUri(reference)} .` : ''}
            ${detail ? `${sparqlEscapeUri(uri)} oslc:largePreview ${sparqlEscapeString(detail)} .` : ''}
        }
      }
  `;
  try {
    await update(q);
  } catch (e) {
    console.error(`[WARN] Something went wrong while trying to store an error.\nMessage: ${e}\nQuery: ${q}`);
  }
}