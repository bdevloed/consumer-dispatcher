export class ProcessingQueue {
  constructor(name = 'Default', config = {}) {
    this.name = name;
    this.queue = [];
    this.executing = false; //To avoid subtle race conditions TODO: is this required?
    this.ready_to_run = false;
    this.config = config;
    this.run();
  }

  async run() {
    if (this.check_ready_to_run()) {
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
    else {
      console.log(`${this.name}: Initial syncs still running, waiting ...`);
      setTimeout(() => { this.run(); }, (process.env.INITIAL_SYNC_POLL_INTERVAL || 10000));
    }
  }

  async check_ready_to_run() {
    if (this.ready_to_run) {
      return true
    } else {
      console.log(JSON.stringify(this))
      if (this.config.prerequisite) {
        this.ready_to_run = this.config.prerequisite.ready();
      } else {
        // No prerequite, so we are ready to run
        this.ready_to_run = true;
      }
      return this.ready_to_run;
    }
  }

  // Are initial dispatches done?
  addJob(origin, onError = async (error) => { console.error(`${this.name}: Error while processing task`, error); }) {
    this.queue.push({
      task: origin,
      onError: onError
    });
  }
}
