import os
import pickle

import src.core.utils as core_utils
from src.config.paths import SCHEDULED_JOBS_PATH


class JobPersistance:
    """Persist scheduled jobs such as /remindme in a pickle file. Jobs are stored in a dict of dicts."""

    def __init__(self, job_queue):
        self.jobs = self.load_jobs(job_queue)

    def load_jobs(self, job_queue):
        if not os.path.exists(SCHEDULED_JOBS_PATH):
            return {}

        with open(SCHEDULED_JOBS_PATH, "rb") as f:
            try:
                self.jobs = pickle.load(f)
            except EOFError:
                self.jobs = {}
        self.sync_jobs()

        for job_id, _ in self.jobs.items():
            self.run_job(job_queue, job_id)

        return self.jobs

    def save_job(self, job_queue, dt, func, args):
        job_id = self.get_new_job_id()
        self.jobs[job_id] = {"dt": dt, "func": func, "args": args}
        self.run_job(job_queue, job_id)
        self.sync_jobs()
        self.save_jobs()

    def save_jobs(self):
        with open(SCHEDULED_JOBS_PATH, "wb") as f:
            pickle.dump(self.jobs, f)

    def sync_jobs(self):
        """Check if persisted jobs are still valid."""
        dt_now = core_utils.get_dt_now()
        new_jobs = {job_id: job for job_id, job in self.jobs.items() if job["dt"] >= dt_now}
        self.jobs = new_jobs

    def get_latest_job_id(self, job_queue):
        return job_queue.jobs()[-1].id

    def get_new_job_id(self):
        return max(self.jobs.keys()) + 1 if self.jobs else 0

    def run_job(self, job_queue, job_id):
        job = self.jobs[job_id]
        job_queue.run_once(callback=lambda context: job["func"](context, *job["args"]), when=job["dt"])
