import { Literal, NamedNode, triple } from "rdflib";
import flatten from "lodash.flatten";
import { findSubjectTypes } from "./queries";

import {
  RDF_TYPE
} from '../constants'

export class Delta {
  constructor(delta) {
    this.delta = delta;
  }

  get inserts() {
    return flatten(this.delta.map((changeSet) => changeSet.inserts));
  }

  get deletes() {
    return flatten(this.delta.map((changeSet) => changeSet.deletes));
  }

  addInserts(inserts) {
    // console.log('addInserts')
    // console.log(inserts)
    // console.log(JSON.stringify(this.delta.inserts));
    // console.log(JSON.stringify(this.delta));
    this.delta.push({
      inserts: inserts,
    });
  }

  addDeletes(deletes) {
    this.delta.push(...deletes);
  }


  /**
   * Create a delta message with additional context needed for further procesing of the data.
   *  TODO: additional query logic
   *
   * @param {Object} delta - The orginal delta to add the types to.
   * @param {Object} config
   * @param {String} config.scope - The scope of the inserts to find the types for. Can be either "inserts", "deletes", "all" or "none". Defaults to "inserts".
   * @param {Boolean} config.find_all - If true, all subjects will be checked for a type. If false, only subjects without a type will be checked. Defaults to false.
   * @param {Array} config.additional_queries - An array of additional queries to execute per subject type. Each query should return a list of triples. Defaults to [].
   * @returns {Delta} - A new Delta object with the types added.
   */
  static async extend(delta, config = { scope: "inserts", find_all: false, additional_queries: [] }) {
    if (!["inserts", "deletes", "all", "none"].includes(config.scope)) {
      throw new Error(`Invalid scope: ${config.scope}`);
    }

    if (["inserts", "all"].includes(config.scope)) {
      let subjects = delta.inserts.map((insert) => insert.subject.value);

      // console.log(`INSERTS:\n${JSON.stringify(d.inserts, null, 2)}`)

      if (config.find_all) {
        let uniqueSubjects = [...new Set(subjects)];
        delta.addInserts(await findSubjectTypes(uniqueSubjects));
      } else {
        // Only find types for subjects that don't have a type
        let subjectsWithoutAType = delta.constructor.subjectsWithoutAType(delta.inserts)
        if (subjectsWithoutAType.length > 0) {
          delta.addInserts(await findSubjectTypes(subjectsWithoutAType));
        }
        // console.log(`FS: ${subjectsWithoutAType}`)
      }
    }
    if (["deletes", "all"].includes(config.scope)) {
      let subjects = delta.deletes.map((delete_) => delete_.subject.value);
      if (config.find_all) {
        let uniqueSubjects = [...new Set(subjects)];
        delta.addDeletes(await findSubjectTypes(uniqueSubjects));
      } else {
        // Only find types for subjects that don't have a type
        let subjectsWithoutAType = delta.constructor.subjectsWithoutAType(delta.deletes)
        if (subjectsWithoutAType.length > 0) {
          delta.addDeletes(await findSubjectTypes(subjectsWithoutAType));
        }
      }
    }
    return delta;
  }


  static subjectsWithoutAType(data) {
    let subjects = data.map((triple) => triple.subject.value);
    let uniqueSubjects = [...new Set(subjects)];
    let subjectsWithAType = data
      .filter(term => term.predicate.value === RDF_TYPE)
      .map(term => term.subject.value)

    let subjectsWithoutAType = uniqueSubjects.filter(subject => !subjectsWithAType.includes(subject));
    return subjectsWithoutAType;
  }

  // TODO: language tagged literals

  static mapToRdfTriple(t) {
    let object;
    if (t.object.type === "uri") {
      object = new NamedNode(t.object.value);
    } else if (t.object.type === "literal") {
      object = new Literal(t.object.value);
    } else if (t.object.type === "typed-literal") {
      object = new Literal(t.object.value, undefined, t.object.datatype);
    }

    return triple(
      new NamedNode(t.subject.value),
      new NamedNode(t.predicate.value),
      object
    );
  }

  get insertTriples() {
    return this.inserts.map((t) => this.mapToRdfTriple(t));
  }

  get deleteTriples() {
    return this.deletes.map((t) => this.mapToRdfTriple(t));
  }
}