import { sparqlEscapeUri, sparqlEscapeString, sparqlEscapeDateTime, uuid } from "mu";
import { query, update } from "mu";
import { querySudo, updateSudo } from "@lblod/mu-auth-sudo";

import {
  BATCH_SIZE,
  SLEEP_BETWEEN_BATCHES,
  MAX_ATTEMPTS,
  SLEEP_TIME_ON_FAIL,
  INGEST_GRAPH,
  INITIAL_DISPATCH_ENDPOINT,
  DELTA_NOTIFICATION_ENDPOINT
} from "../config";

import {
  PREFIXES,
  STATUS_BUSY,
  STATUS_SCHEDULED,
  STATUS_SUCCESS,
  STATUS_FAILED,
  RDF_TYPE,
  MU_UUID
} from "../constants";

const CREATOR = 'http://lblod.data.gift/services/loket-consumer-dispatcher-service';


export async function insertIntoGraph(graph, triples) {
  const data = [... new Set(triples)];
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

const values_template = (variable, values) => values ? `
VALUES ?${variable} {
  ${Array.isArray(values) ? values.map(v => sparqlEscapeUri(v)).join('\n') : sparqlEscapeUri(values)}
 }` : '';

export const operation_status_query = (operation, status) => `
${PREFIXES}
SELECT DISTINCT ?s ?created WHERE {
  ${values_template('operation', operation)}
  ${values_template('status', status)}
  ?s a <http://vocab.deri.ie/cogs#Job> ;
    task:operation ?operation ;
    adms:status ?status ;
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

    const batch = triples.slice(i, i + BATCH_SIZE);

    const insertCall = async () => {
      await updateSudo(update_template(graph, batch));
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
      const result = await querySudo(operation_status_query(operation, STATUS_SUCCESS));

      console.log(`Result for ${operation}: ${JSON.stringify(result)} `);

      const initial_sync_done = !!(result && result.results.bindings.length);
      if (!initial_sync_done) {
        console.log(`Initial sync for ${operation} not done yet.`);
        return false;
      } else {
        console.log(`Initial sync for ${operation} done.`);
      }
    }
    return true;
  } catch (e) {
    const error_message = `Error while checking if initial syncs are done: ${e.message ? e.message : e} `;
    console.log(error_message);
    sendErrorAlert({
      message: error_message
    });
    return false;
  }
}

// TODO move?
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

const initial_dispatch_query = (graph, types) => `
INSERT {
  GRAPH ${sparqlEscapeUri(graph)} {
    ?s ?p ?o.
  }
}
WHERE {
  GRAPH ${sparqlEscapeUri(INGEST_GRAPH)} {
    ${values_template('type', types)}
    ?s
      a ?type;
      ?p ?o.
  }
}`

const types_and_uuids_query = (types) => `
${PREFIXES}
SELECT ?s ?type ?uuid WHERE {
  GRAPH ${sparqlEscapeUri(INGEST_GRAPH)} {
    ${values_template('type', types)}
    ?s
      a ?type.
    OPTIONAL {
      ?s mu:uuid ?uuid.
    }
  }
} `


// Directly execute the query - bypassing mu-auth - when DIRECT_DATABASE_ENDPOINT is set
export async function distpatch_all(graph, types) {
  await updateSudo(
    initial_dispatch_query(graph, types),
    {
      // no extraheaders
    },
    {
      sparqlEndpoint: INITIAL_DISPATCH_ENDPOINT
    }
  );
}

export async function dispatch_for_indexing(graph, types) {
  let types_and_uuids = await querySudo(
    types_and_uuids_query(types),
    {},
    {
      sparqlEndpoint: INITIAL_DISPATCH_ENDPOINT
    })

  let inserts = types_and_uuids.results.bindings.map(
    r => ([
      {
        'graph': {
          'value': graph,
          'type': 'uri'
        },
        'subject': r.s,
        'predicate': {
          'value': RDF_TYPE,
          'type': 'uri'
        },
        'object': r.type
      }, {
        'graph': {
          'value': graph,
          'type': 'uri'
        },
        'subject': r.s,
        'predicate': {
          'value': MU_UUID,
          'type': 'uri'
        },
        'object': r.uuid
      }
    ])
  ).flat();

  // console.log(`Deltas: ${JSON.stringify(deltas)}`);
  console.log(`Dispatching ${inserts.length} deltas for indexing`);
  await delta_notification(inserts, [])
}

async function delta_notification(inserts = [], deletes = []) {
  const delta = {
    'changeSets': [{
      'insert': inserts,
      'delete': deletes
    }]
  };

  // console.log(`Dispatching delta: ${JSON.stringify(delta)}`);

  await fetch(
    DELTA_NOTIFICATION_ENDPOINT,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: JSON.stringify(delta)
    }
  ).then(function (response) {
    if (!response.ok) {
      throw new Error(`Error while dispatching delta: ${response.status} ${response.statusText}`);
    } else {
      console.log(`Delta dispatched successfully`);
    }
  }).catch(function (err) {
    throw new Error(`Error while dispatching delta: ${err} `);
  });

}


export async function findSubjectTypes(subjects) {
  return await querySudo(subject_type_query(INGEST_GRAPH, subjects))
    .then(function (response) {
      return response.results.bindings.map(
        r => ({
          'subject': r.subject,
          'predicate': {
            'value': RDF_TYPE,
            'type': 'uri'
          },
          'object': r.type
        })
      )
    })
    .catch(function (err) {
      throw new Error(`Error while querying for types: ${err} `);
    });
}
