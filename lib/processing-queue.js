import {
  all_initial_syncs_done
} from '../util/queries';

export class ProcessingQueue {
  constructor(name = 'Default') {
    this.name = name;
    this.queue = [];
    this.run();
    this.executing = false; //To avoid subtle race conditions TODO: is this required?
    this.all_initial_syncs_done = false;
  }

  async run() {
    // first deltas are expected to come from initial syncs. Check whether they are completed before processing the queue
    if (!this.syncs_ready) {
      console.log(`${this.name}: Checking for any running initial syncs`);
      this.syncs_ready = await all_initial_syncs_done();
      console.log(`${this.name}: Initial syncs ready: ${this.syncs_ready}`)
      if (this.syncs_ready) {
        console.log(`${this.name}: Initial syncs completed, processing queue`);
      } else {
        console.log(`${this.name}: Initial syncs still running, waiting ...`);
        setTimeout(() => { this.run(); }, (process.env.INITIAL_SYNC_POLL_INTERVAL || 10000));
      }
    } else {
      if (this.queue.length > 0 && !this.executing) {
        const job = this.queue.shift();
        try {
          this.executing = true;
          console.log(`${this.name}: Executing oldest task on queue`);
          await job.task();
          console.log(`${this.name}: Remaining number of tasks ${this.queue.length}`);
        }
        catch (error) {
          await job.onError(error);
        }
        finally {
          this.executing = false;
          this.run();
        }
      }
      else {
        setTimeout(() => { this.run(); }, (process.env.QUEUE_POLL_INTERVAL || 100));
      }
    }
  }

  addJob(origin, onError = async (error) => { console.error(`${this.name}: Error while processing task`, error); }) {
    this.queue.push({
      task: origin,
      onError: onError
    });
  }
}