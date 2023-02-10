import bodyParser from "body-parser";
import { app } from "mu";
import { Delta } from "./lib/delta";
import { n3reasoner } from 'eyereasoner';
import { insertIntoGraph } from "./util/queries";
import { dispatch_config } from "./dispatch-config";
import { ProcessingQueue } from './lib/processing-queue';

const processSubjectsQueue = new ProcessingQueue('loket-consumer-dispatch-queue');



import {
  RDF_TYPE
} from './constants'

// import { ProcessingQueue } from './lib/processing-queue';
// import {
//   sendErrorAlert,
//   getTypesForSubject,
//   getWorshipAdministrativeUnitForSubject,
//   getDestinationGraphs,
//   copySubjectDataToDestinationGraphs,
//   getRelatedSubjectsForWorshipAdministrativeUnit
// } from "./util/queries";
// import exportConfig from "./export-config";

// const processSubjectsQueue = new ProcessingQueue('worship-positions-process-queue');
// const dispatchSubjectsQueue = new ProcessingQueue('worship-positions-dispatch-queue');

// This service stores incoming data in a separate ingest graph.
// This graph is then used to determine which data should be copied to the destination graphs.
// This is done to prevent that unnecessary data is copied to the destination graphs.
// The ingest graph is then cleared, only the rdf:type statements are kept.


app.use(
  bodyParser.json({
    type: function (req) {
      return /^application\/json/.test(req.get("content-type"));
    },
    limit: '50mb',
    extended: true
  })
);

app.use(
  bodyParser.urlencoded({
    type: function (req) {
      return /^application\/json/.test(req.get("content-type"));
    },
    limit: '50mb',
    extended: true
  })
);

app.get("/", async function (req, res) {
  res.send("Hello from the op consumer-filter service!");
});

app.post("/delta", async function (req, res) {
  // TODO: send 204 for empty - rewrite Delta construction
  processSubjectsQueue.addJob(() => dispatch(Delta.create(req.body)));
  return res.status(200).send();
});


async function map(delta) {
  return delta
}

async function dispatch(delta) {
  // create a hashmap of all subjects and their types

  let typeMap = new Map();
  delta.inserts
    .filter(term => term.predicate.value === RDF_TYPE)
    .forEach(element => {
      if (!typeMap.has(element.subject.value)) {
        typeMap.set(element.subject.value, []);
      }
      typeMap.get(element.subject.value).push(element.object.value);
    });


  dispatch_config.forEach(({ graph, types }) => {
    // subjects of interest are those that have at least one of the types in the filter config
    let subjectsToDispach = [...typeMap.keys()].filter(
      subject => typeMap.get(subject).some(r => types.includes(r))
    )

    let inserts_to_dispatch = delta.inserts.filter(
      term => subjectsToDispach.includes(term.subject.value)
    )
    if (inserts_to_dispatch.length > 0) {
      insertIntoGraph(graph, inserts_to_dispatch.map(term => Delta.mapToRdfTriple(term).toNT()))
    }
  });

}


