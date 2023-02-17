import { Dispatcher } from './dispatcher.js';
import { isDatabaseUp } from './database.js';
import {
  all_initial_syncs_done,
} from './queries.js';

export class Prerequisite {
  constructor() {
    this._ready = false;
    this.initial_dispatch_busy = false;
    this.dispatcher = new Dispatcher('initial_dispatch');
    this.db_up = false;
  }

  async ready() {
    if (this._ready) {
      return this._ready;
    } else {
      if (!this.db_up) {
        this.db_up = await isDatabaseUp();
        if (!this.db_up) {
          console.log("Waiting for database... ")
          return false;
        }
      }
      let syncs_done = await all_initial_syncs_done();
      if (syncs_done) {
        if (!this.initial_dispatch_busy) {
          console.log("Initial syncs done, starting initial dispatch");
          this.initial_dispatch_busy = true;
          this._ready = await this.dispatcher.initial_dispatch();
          this.initial_dispatch_busy = false;
          return this._ready;
        } else {
          console.log("Initial dispatch already running, waiting for it to finish");
          return false;
        }
      } else {
        console.log("Waiting for initial syncs to finish... ");
        return false;
      }
    }
  }
}