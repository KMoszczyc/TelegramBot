import os
import pickle
from functools import wraps

from definitions import SCHEDULED_JOBS_PATH


class JobPersistance:
    def __init__(self, job_queue):
        self.jobs = self.load_jobs(job_queue)

    def load_jobs(self, job_queue):
        if not os.path.exists(SCHEDULED_JOBS_PATH):
            return {}

        with open(SCHEDULED_JOBS_PATH, 'rb') as f:
            try:
                self.jobs = pickle.load(f)
            except EOFError:
                self.jobs = {}

        for job_id, _ in self.jobs.items():
            self.run_job(job_queue, job_id)

        print(self.jobs)

        return self.jobs

    def save_job(self, job_queue, dt, func, args):
        # job_id = self.get_new_job_id()
        placeholder_id=0
        self.jobs[placeholder_id] = {'dt': dt, 'func': func, 'args': args}
        self.run_job(job_queue, placeholder_id)
        # print('job_queue', job_queue.jobs())
        # print('before', self.jobs)
        self.sync_jobs(job_queue)
        # print('after', self.jobs)

        self.save_jobs()

    def save_jobs(self):
        print('saving jobs', self.jobs)
        with open(SCHEDULED_JOBS_PATH, 'wb') as f:
            pickle.dump(self.jobs, f)

    def sync_jobs(self, job_queue):
        """TODO: possible issue here"""
        latest_job_id = self.get_latest_job_id(job_queue)
        self.jobs[latest_job_id] = self.jobs.pop(0)
        new_jobs = {}
        for job in job_queue.jobs():
            if job.id in self.jobs:
                new_jobs[job.id] = self.jobs[job.id]

        self.jobs = new_jobs

    def get_latest_job_id(self, job_queue):
        return job_queue.jobs()[-1].id

    def get_new_job_id(self):
        return max(self.jobs.keys()) + 1 if self.jobs else 0

    def run_job(self, job_queue, job_id):
        job = self.jobs[job_id]
        # context.job_queue.run_once(callback=lambda context: self.delete_job_decorator(job_id)(job['func'](context, *job['args'])), when=job['dt'])
        job_queue.run_once(callback=lambda context: job['func'](context, *job['args']), when=job['dt'])

    def delete_job_decorator(self, job_id):
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                result = await func(*args, **kwargs)

                del self.jobs[job_id]
                with open(SCHEDULED_JOBS_PATH, 'wb') as f:
                    pickle.dump(self.jobs, f)

                return result

            return wrapper

        return decorator
