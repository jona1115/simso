"""
Implementation of the Global-EDF (Earliest Deadline First) for multiprocessor
architectures.
"""
from simso.core import Scheduler
from simso.schedulers import scheduler

@scheduler("simso.schedulers.MK_EDF")
class MK_EDF(Scheduler):
    """Earliest Deadline First"""
    def on_activate(self, job):
        job.cpu.resched()

    def on_terminated(self, job):
        job.cpu.resched()

    def schedule(self, cpu):
        # List of ready jobs not currently running:
        ready_jobs = [t.job for t in self.task_list
                      if t.is_active() and not t.job.is_running()]

        if ready_jobs:
            ############### As far as I can tell this chunk is just selecting a CPU, so for uniprocessor, this chunk is junk ###############
            # Select a free processor or, if none,
            # the one with the greatest deadline (self in case of equality):
            key = lambda x: (
                1 if not x.running else 0,
                x.running.absolute_deadline if x.running else 0,
                1 if x is cpu else 0
            )
            cpu_min = max(self.processors, key=key)
            ####################################################### end junk chunk #######################################################


            # Select the job with the least priority:
            job = min(ready_jobs, key=lambda x: (x.optional, x.absolute_deadline, x.period))

            if ((# If there is nothing running rn, or
                 (cpu_min.running is None) or
                 # if the current running is optional and the job is mandatory, also if the current running deadline < job deadline, or
                 ((cpu_min.running is not None and job is not None) and (cpu_min.running.optional and job.mandatory) and (cpu_min.running.absolute_deadline < job.absolute_deadline)) or
                 # if the current running deadline is > the job's deadline
                 (cpu_min.running.absolute_deadline > job.absolute_deadline)
                ) and
                # finally we make sure a mandatory task don't get preempted by a optional task
                not ((cpu_min.running is not None and cpu_min.running.mandatory) and job.optional)
               ):

                print(self.sim.now()/1000000, job.name, cpu_min.name)
                return (job, cpu_min)
