from __future__ import absolute_import
from datetime import datetime


from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.util import datetime_to_utc_timestamp, utc_timestamp_to_datetime
from apscheduler.job import Job

try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle

try:
    from elasticsearch import Elasticsearch
    from elasticsearch.exceptions import NotFoundError
    from elasticsearch import helpers
except ImportError:  # pragma: nocover
    raise
    raise ImportError('ElasticsearchJobStore requires elasticsearch installed')



class ElasticsearchJobStore(BaseJobStore):
    """
    Stores jobs in an Elasticsearch index. Any leftover keyword arguments are directly passed to elasticsearch's
    :class:`~elasticsearch.Elasticsearch`.

    Plugin alias: ``elasticsearch``

    :param str index: the index to store jobs in
    :param str run_times_key: key to store the jobs' run times in
    :param client: a :class:`~elasticsearch.Elasticsearch` instance to use instead of providing connection arguments
    :param int pickle_protocol: pickle protocol level to use (for serialization), defaults to the
        highest available
    """

    def __init__(self, index='apscheduler', run_times_key='apscheduler_run_times',
                 content_key='apscheduler_job_content', client=None, pickle_protocol=pickle.HIGHEST_PROTOCOL,
                 **connect_args):
        super(ElasticsearchJobStore, self).__init__()

        if index is None:
            raise ValueError('The "index" parameter must not be empty')
        if run_times_key is None:
            raise ValueError('The "run_times_key" parameter must not be empty')
        if content_key is None:
            raise ValueError('The "content_key" parameter must not be empty')

        self.index = index
        self.run_times_key = run_times_key
        self.content_key = content_key

        if client:
            self.client = maybe_ref(client)
        else:
            self.client = Elasticsearch(**connect_args)


    def _reconstitute_job(self, job_state):
        job_state = job_state
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        return job


    def lookup_job(self, job_id):
        """
        Returns a specific job, or ``None`` if it isn't found..

        The job store is responsible for setting the ``scheduler`` and ``jobstore`` attributes of
        the returned job to point to the scheduler and itself, respectively.

        :param str|unicode job_id: identifier of the job
        :rtype: Job
        """
        try:
            source = self.client.get(index=index, id=job_id)["_source"]
            job_state = pickle.loads(source[self.content_key])
            return self._reconstitute_job(job_state)
        except NotFoundError:
            return None

    def get_due_jobs(self, now):
        """
        Returns the list of jobs that have ``next_run_time`` earlier or equal to ``now``.
        The returned jobs must be sorted by next run time (ascending).

        :param datetime.datetime now: the current (timezone aware) datetime
        :rtype: list[Job]
        """
        elasticsearch_timestamp = now.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        query = {
            "query" : {
                "range" : {
                    self.run_times_key: {
                        "lte" : elasticsearch_timestamp
                        }
                    }
                },
                "sort" : [
                    {
                        self.run_times_key : { "order" : "desc"}
                    }
                ]
            }
        return self._readAllJobsForQuery(query)


    def _readAllJobsForQuery(self, query):
        jobs = []
        bulkreader = helpers.scan(self.client, query=query, index=self.index, preserve_order=True)
        for job in bulkreader:
            job_state = pickle.loads(job["source"][self.content_key])
            jobs.append(self._reconstitute_job(job_state))
        return jobs


    def get_next_run_time(self):
        """
        Returns the earliest run time of all the jobs stored in this job store, or ``None`` if
        there are no active jobs.

        :rtype: datetime.datetime
        """
        query = {
            "query" : {
                "match_all" : {}
                },
            "sort" : [
                {
                    self.run_times_key : { "order" : "desc"}
                }
            ],
            "count" : 1
        }
        results = self.client.search(index=self.index, query=query)
        if results["hits"]["hits"]:
            result = results["hits"]["hits"][0]["_source"]
            return datetime.strptime(result[self.run_times_key], '%Y-%m-%dT%H:%M:%S.%fZ')
        else:
            return None


    def get_all_jobs(self):
        """
        Returns a list of all jobs in this job store.
        The returned jobs should be sorted by next run time (ascending).
        Paused jobs (next_run_time == None) should be sorted last.

        The job store is responsible for setting the ``scheduler`` and ``jobstore`` attributes of
        the returned jobs to point to the scheduler and itself, respectively.

        :rtype: list[Job]
        """
        query = {
            "query" : {
                "match_all" : {}
                },
            "sort" : [
                {
                    self.run_times_key : { "order" : "desc"}
                }
            ]
        }
        return self._readAllJobsForQuery(query)


    def add_job(self, job):
        """
        Adds the given job to this store.

        :param Job job: the job to add
        :raises ConflictingIdError: if there is another job in this store with the same ID
        """
        if lookup_job(job.id) != None:
            raise ConflictingIdError(job.id)
        else:
            self._put_job(job)


    def update_job(self, job):
        """
        Replaces the job in the store with the given newer version.

        :param Job job: the job to update
        :raises JobLookupError: if the job does not exist
        """
        if lookup_job(job.id) == None:
            raise JobLookupError(job.id)
        else:
            self._put_job(job)

    def _put_job(self, job):
        source = {}
        source[self.content_key] = pickle.dumps(job, self.pickle_protocol)
        source[self.run_times_key] = job.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        self.client.index(index=self.index, body=source, doc_type="ap", id=job.id)


    def remove_job(self, job_id):
        """
        Removes the given job from this store.

        :param str|unicode job_id: identifier of the job
        :raises JobLookupError: if the job does not exist
        """
        try:
            self.client.delete(index=self.index, id=job_id, doc_type="ap")
        except NotFoundError:
            raise JobLookupError(job_id)


    def remove_all_jobs(self):
        """Removes all jobs from this store."""
        for job in self.get_all_jobs():
            remove_job(job.id)

    def __repr__(self):
        self._logger.exception('<%s (client=%s)>' % (self.__class__.__name__, self.client))
        return '<%s (client=%s)>' % (self.__class__.__name__, self.client)
