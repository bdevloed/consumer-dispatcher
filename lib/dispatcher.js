import { dispatch_config } from "../dispatch-config";
import { initial_dispatch } from "../util/queries";

import { createJob, getLatestJobForOperation } from './job';
import { createTask } from '../lib/task';
import {
  INITIAL_DISPATCH_JOB_OPERATION,
  INITIAL_DISPATCH_TASK_OPERATION,
  JOB_CREATOR_URI,
  JOBS_GRAPH,
  STATUS_BUSY,
  STATUS_FAILED,
  STATUS_SCHEDULED,
  STATUS_SUCCESS
} from '../constants';

import { updateStatus } from "./utils";

export class Dispatcher {
  constructor(name) {
    this.name = name;
    this.config = dispatch_config;
  }

  async initial_dispatch() {
    let job;
    let task;

    try {
      let initialDispatchJob = await getLatestJobForOperation(INITIAL_DISPATCH_JOB_OPERATION, JOB_CREATOR_URI);
      if (!initialDispatchJob || initialDispatchJob.status == STATUS_FAILED) {

        // Note: they get status busy
        job = await createJob(JOBS_GRAPH, INITIAL_DISPATCH_JOB_OPERATION, JOB_CREATOR_URI, STATUS_BUSY);
        task = await createTask(JOBS_GRAPH, job, "0", INITIAL_DISPATCH_TASK_OPERATION, STATUS_SCHEDULED);

        await this.config.forEach(async ({ graph, types }) => {
          await types.forEach(async (type) => {
            await initial_dispatch(graph, type)
          });
        });

        await updateStatus(job, STATUS_SUCCESS);
        return job;
      } else {
        console.log(`Initial dispatch already done, skipping.`);
        return initialDispatchJob;
      }
    }
    catch (e) {
      console.log(`Something went wrong while doing the initial dispatch. Closing task with failure state.`);
      console.trace(e);
      if (task)
        await updateStatus(task, STATUS_FAILED);
      if (job) {
        await createJobError(JOBS_GRAPH, job, e);
        await updateStatus(job, STATUS_FAILED);
      }
      throw e;
    }
  }

  async dispatch(delta) {
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


    this.config.forEach(({ graph, types }) => {
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
}