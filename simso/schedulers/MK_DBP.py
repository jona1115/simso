from collections import deque

from simso.core import Scheduler
from simso.schedulers import scheduler


@scheduler("simso.schedulers.MK_DBP")
class MK_DBP(Scheduler):
    """
    Distance Based Priority scheduler for (m,k)-firm tasks.

    Based on: Joël Goossens. (m, k)-firm constraints and DBP scheduling: impact of the initial 
    k-sequence and exact schedulability test. 16th International Conference on Real-Time and 
    Network Systems (RTNS 2008), Isabelle Puaut, Oct 2008, Rennes, France. ⟨inria-00336461⟩

    This is coded to work on Uniprocessor simulations!
    """

    def init(self):
        self.ready_list = []
        self._k_histories = {}
        for task in self.task_list:
            self._k_histories[task] = self._init_history(task)

    def on_activate(self, job):
        self.ready_list.append(job)
        job.cpu.resched()

    def on_terminated(self, job):
        self._update_history(job)
        if job in self.ready_list:
            self.ready_list.remove(job)
        else:
            job.cpu.resched()

    def _should_run(self, candidate, current):
        if current is None:
            return True
        if current.mandatory and candidate.optional:
            return False
        return self._priority_tuple(candidate) < self._priority_tuple(current)

    def _priority_tuple(self, job):
        return (
            job.optional,                       # mandatory > optional
            self._compute_distance(job.task),   # distance
            job.absolute_deadline,              # on tie use EDF
            job.period                          # then RMS
        )

    def _priority_distance(self, job):
        if job is None:
            return float("inf")
        return self._compute_distance(job.task)

    def _compute_distance(self, task):
        """
        Distance, as per Goossens 2008, is defined as starting from time now, the amount of
        deadline we can affort to miss before we violate the (m,k)-firm requirement.

        In practice, we achieve this but keeping track of the history of the task as a list.
        if successes is < m the distance is 0 because it is already violated (m,k)-firm
        requirement. Else, it append a hypothetical miss into the list, then it runs the check 
        again.
        """
        history = self._k_histories.get(task)
        if history is None:
            history = self._init_history(task)
            self._k_histories[task] = history
        m = getattr(task._task_info, "m", 1)
        if m <= 0:
            return float("inf")
        sequence = list(history)
        successes = sum(sequence)
        if successes < m:
            return 0
        allowed = 0
        current = sequence[:]
        k = len(current)
        while allowed < k:
            current = current[1:] + [0]
            allowed += 1
            if sum(current) < m:
                return allowed - 1
        return allowed

    def _init_history(self, task):
        k = max(1, getattr(task._task_info, "k", 1))
        initial = None
        if task.data and isinstance(task.data, dict):
            initial = task.data.get("initial_k_sequence")
        values = self._normalize_init_sequence(initial)
        history = deque(maxlen=k)
        slice_start = max(0, len(values) - k)
        for value in values[slice_start:]:
            history.append(value)
        while len(history) < k:
            history.appendleft(1)
        return history

    @staticmethod
    def _normalize_init_sequence(initial):
        if initial is None:
            return [1]
        result = []
        if isinstance(initial, str):
            for char in initial:
                if char == "1":
                    result.append(1)
                elif char == "0":
                    result.append(0)
        elif isinstance(initial, (list, tuple, deque)):
            for value in initial:
                result.append(1 if value else 0)
        elif isinstance(initial, bool):
            result.append(1 if initial else 0)
        elif isinstance(initial, (int, float)):
            result.append(1 if initial else 0)
        else:
            result.append(1)
        if not result:
            result.append(1)
        return result

    def _update_history(self, job):
        task = job.task
        history = self._k_histories.get(task)
        if history is None:
            history = self._init_history(task)
            self._k_histories[task] = history
        history.append(0 if job.exceeded_deadline else 1)

    def schedule(self, cpu):
        if not self.ready_list:
            return None

        key = lambda x: (
            0 if x.running is None else 1,
            -self._priority_distance(x.running) if x.running else 0,
            0 if x is cpu else 1,
        )
        cpu_min = min(self.processors, key=key)

        job = min(self.ready_list, key=self._priority_tuple)

        if self._should_run(job, cpu_min.running):
            self.ready_list.remove(job)
            if cpu_min.running:
                self.ready_list.append(cpu_min.running)
            print(self.sim.now() / 1000000, job.name, cpu_min.name)
            return (job, cpu_min)

        return None