import { Dispatcher } from './dispatcher.js';
import {
  all_initial_syncs_done,
} from '../util/queries.js';

export class Prerequisite {
  constructor() {
    this._ready = false;
    this.initial_dispatch_busy = false;
    this.dispatcher = new Dispatcher('initial_dispatch');
  }

  async ready() {
    if (this._ready) {
      return this._ready;
    } else {
      let syncs_done = await all_initial_syncs_done();
      if (syncs_done && !this.initial_dispatch_busy) {
        this.initial_dispatch_busy = true;
        this._ready = await this.dispatcher.initial_dispatch();
        this.initial_dispatch_busy = false;
        return this._ready;
      } else {
        return false;
      }
    }
  }
}