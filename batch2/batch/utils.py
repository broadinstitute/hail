import re
import time
import logging

from hailtop.batch_client.validate import CPU_REGEX, MEMORY_REGEX


log = logging.getLogger('utils')


def parse_cpu_in_mcpu(cpu_string):
    match = CPU_REGEX.fullmatch(cpu_string)
    if match:
        number = float(match.group(1))
        if match.group(2) == 'm':
            number /= 1000
        return int(number * 1000)
    return None


conv_factor = {
    'K': 1000, 'Ki': 1024,
    'M': 1000**2, 'Mi': 1024**2,
    'G': 1000**3, 'Gi': 1024**3,
    'T': 1000**4, 'Ti': 1024**4,
    'P': 1000**5, 'Pi': 1024**5
}


def parse_memory_in_bytes(memory_string):
    match = MEMORY_REGEX.fullmatch(memory_string)
    if match:
        number = float(match.group(1))
        suffix = match.group(2)
        return int(number * conv_factor[suffix])
    return None


image_regex = re.compile(r"(?:.+/)?([^:]+)(:(.+))?")


def parse_image_tag(image_string):
    match = image_regex.fullmatch(image_string)
    if match:
        return match.group(3)
    return None


class LoggingTimerStep:
    def __init__(self, timer, name):
        self.timer = timer
        self.name = name
        self.start_time = None

    async def __aenter__(self):
        self.start_time = time.time()

    async def __aexit__(self, exc_type, exc, tb):
        finish_time = time.time()
        self.timer.timing[self.name] = finish_time - self.start_time


class LoggingTimer:
    def __init__(self, description):
        self.description = description
        self.timing = {}
        self.start_time = None

    def step(self, name):
        return LoggingTimerStep(self, name)

    async def __aenter__(self):
        self.start_time = time.time()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        finish_time = time.time()
        self.timing['total'] = finish_time - self.start_time

        log.info(f'{self.description} timing {self.timing}')
