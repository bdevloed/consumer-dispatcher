import bodyParser from "body-parser";
import { app } from "mu";
import { Delta } from "./lib/delta";
import { ProcessingQueue } from "./lib/processing-queue";
import { Prerequisite } from "./lib/prerequisite";
import { Dispatcher } from "./lib/dispatcher";

const queue = new ProcessingQueue('loket-consumer-dispatch-queue', {
  prerequisite: new Prerequisite()
});

const dispatcher = new Dispatcher('loket-consumer-dispatcher');


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
  const delta = new Delta(req.body);

  if (!delta.inserts.length) {
    console.log(
      "Delta does not contain any insertions. Nothing should happen here. Consumer handles deletions."
    );
    return res.status(204).send();
  }

  console.log("Received delta. Adding it to the processing queue...");
  queue.addJob(() => dispatcher.dispatch(delta));

  return res.status(200).send();
});
