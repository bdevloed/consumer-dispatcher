export const RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
export const MU_UUID = 'http://mu.semte.ch/vocabularies/core/uuid';

export const INITIAL_DISPATCH_JOB_OPERATION = 'http://lblod.data.gift/vocabularies/jobs/InitialDispatchJobOperation';
export const INITIAL_DISPATCH_TASK_OPERATION = 'http://lblod.data.gift/vocabularies/tasks/InitialDispatchTaskOperation';
export const JOB_CREATOR_URI = 'http://lblod.data.gift/services/loket-consumer-dispatcher-service';
export const JOBS_GRAPH = 'http://mu.semte.ch/graphs/dispatcher-jobs';

export const JOB_TYPE = 'http://vocab.deri.ie/cogs#Job';
export const TASK_TYPE = 'http://redpencil.data.gift/vocabularies/tasks/Task';
export const ERROR_TYPE = 'http://open-services.net/ns/core#Error';

export const STATUS_BUSY = 'http://redpencil.data.gift/id/concept/JobStatus/busy';
export const STATUS_SCHEDULED = 'http://redpencil.data.gift/id/concept/JobStatus/scheduled';
export const STATUS_SUCCESS = 'http://redpencil.data.gift/id/concept/JobStatus/success';
export const STATUS_FAILED = 'http://redpencil.data.gift/id/concept/JobStatus/failed';


export const JOB_URI_PREFIX = 'http://redpencil.data.gift/id/jobs/';
export const TASK_URI_PREFIX = 'http://redpencil.data.gift/id/task/';
export const ERROR_URI_PREFIX = 'http://redpencil.data.gift/id/jobs/error/';
export const DISPATCH_ERROR_TYPE = 'http://redpencil.data.gift/vocabularies/dispatcher/Error';


export const PREFIXES = `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX oslc: <http://open-services.net/ns/core#>
  PREFIX cogs: <http://vocab.deri.ie/cogs#>
  PREFIX adms: <http://www.w3.org/ns/adms#>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX dbpedia: <http://dbpedia.org/resource/>
`;