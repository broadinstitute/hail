import asyncio
import collections
import logging

log = logging.getLogger('semaphore')


class FIFOWeightedSemaphoreFull(Exception):
    pass


class ANullContextManager:
    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc, tb):
        pass


class NullWeightedSemaphore:
    def __call__(self, weight, id):
        return ANullContextManager()


class FIFOWeightedSemaphoreContextManager:
    def __init__(self, sem, weight):
        self.sem = sem
        self.weight = weight

    async def __aenter__(self, nowait=False):
        await self.sem.acquire(self.weight, nowait=nowait)

    async def __aexit__(self, exc_type, exc, tb):
        if isinstance(exc_type, FIFOWeightedSemaphoreFull):
            return
        self.sem.release(self.weight)


class FIFOWeightedSemaphore:
    def __init__(self, value=1):
        self.value = value
        self.queue = collections.deque()

    async def acquire(self, weight, nowait=False):
        log.info(f'queue {self.queue} value {self.value} weight {weight}')
        if not self.queue and self.value >= weight:
            self.value -= weight
            return

        if nowait and self.value < weight:
            raise FIFOWeightedSemaphoreFull(f'not enough space to acquire semaphore: weight={weight} value={self.value}')

        event = asyncio.Event()
        self.queue.append((event, weight))
        event.clear()
        await event.wait()

    def release(self, weight):
        self.value += weight
        n_notified = 0
        while self.queue:
            head_event, head_weight = self.queue[0]
            if self.value >= head_weight:
                head_event.set()
                self.queue.popleft()
                self.value -= head_weight
                n_notified += 1
            else:
                break

    def __call__(self, weight):
        return FIFOWeightedSemaphoreContextManager(self, weight)
