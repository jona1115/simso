from simso.core import Scheduler
from simso.schedulers import scheduler

from math import floor, ceil

@scheduler("simso.schedulers.MK_RMS")
class MK_RMS(Scheduler):
    """ Rate monotonic """
    def init(self):
        self.ready_list = []

    def on_activate(self, job):
        self.ready_list.append(job)
        job.cpu.resched()

    def on_terminated(self, job):
        if job in self.ready_list:
            self.ready_list.remove(job)
        else:
            job.cpu.resched()

    def schedule(self, cpu):
        decision = None
        if self.ready_list:
            ############### As far as I can tell this chunk is just selecting a CPU, so for uniprocessor, this chunk is junk ###############
            # Get a free processor or a processor running a low priority job.
            key = lambda x: ( # this lambda func aparently returns a 3 element tuple
                0 if x.running is None else 1,
                -x.running.period if x.running else 0,
                0 if x is cpu else 1
            )
            cpu_min = min(self.processors, key=key) # with one cpu, cpu min will just be the only cpu
            ####################################################### end junk chunk #######################################################

            # Job with highest priority.
            job = min(self.ready_list, key=lambda x: (x.optional, x.period))

            if (self.sim.now()/1000000 >= 25.0):
                pass

            if (((cpu_min.running is None) or (cpu_min.running.period > job.period)) and not ((cpu_min.running is not None and cpu_min.running.mandatory) and job.optional)):
                self.ready_list.remove(job)
                if cpu_min.running:
                    self.ready_list.append(cpu_min.running) # if it got preempted, add it back to the que
                decision = (job, cpu_min)
                print(self.sim.now()/1000000, job.name, cpu_min.name)

        return decision
