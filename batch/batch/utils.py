import re
import logging
import math

from .front_end.validate import CPU_REGEX, MEMORY_REGEX

log = logging.getLogger('utils')


def round_up_division(numerator, denominator):
    return (numerator + denominator - 1) // denominator


def coalesce(x, default):
    if x is not None:
        return x
    return default


def cost_str(cost):
    if cost is None:
        return None
    return f'${cost:.4f}'


def cost_from_msec_mcpu(msec_mcpu):
    if msec_mcpu is None:
        return None

    worker_type = 'standard'
    worker_cores = 16
    worker_disk_size_gb = 100

    # https://cloud.google.com/compute/all-pricing

    # per instance costs
    # persistent SSD: $0.17 GB/month
    # average number of days per month = 365.25 / 12 = 30.4375
    avg_n_days_per_month = 30.4375

    disk_cost_per_instance_hour = 0.17 * worker_disk_size_gb / avg_n_days_per_month / 24

    ip_cost_per_instance_hour = 0.004

    instance_cost_per_instance_hour = disk_cost_per_instance_hour + ip_cost_per_instance_hour

    # per core costs
    if worker_type == 'standard':
        cpu_cost_per_core_hour = 0.01
    elif worker_type == 'highcpu':
        cpu_cost_per_core_hour = 0.0075
    else:
        assert worker_type == 'highmem'
        cpu_cost_per_core_hour = 0.0125

    service_cost_per_core_hour = 0.01

    total_cost_per_core_hour = (
        cpu_cost_per_core_hour
        + instance_cost_per_instance_hour / worker_cores
        + service_cost_per_core_hour)

    return (msec_mcpu * 0.001 * 0.001) * (total_cost_per_core_hour / 3600)


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
        if suffix:
            return math.ceil(number * conv_factor[suffix])
        return math.ceil(number)
    return None


def parse_storage_in_bytes(storage_string):
    return parse_memory_in_bytes(storage_string)


def worker_memory_per_core_gb(worker_type):
    if worker_type == 'standard':
        m = 3.75
    elif worker_type == 'highmem':
        m = 6.5
    else:
        assert worker_type == 'highcpu', worker_type
        m = 0.9
    return m


def worker_memory_per_core_bytes(worker_type):
    m = worker_memory_per_core_gb(worker_type)
    return int(m * 1024**3)


def memory_bytes_to_cores_mcpu(memory_in_bytes, worker_type):
    return math.ceil((memory_in_bytes / worker_memory_per_core_bytes(worker_type)) * 1000)


def cores_mcpu_to_memory_bytes(cores_in_mcpu, worker_type):
    return int((cores_in_mcpu / 1000) * worker_memory_per_core_bytes(worker_type))


def adjust_cores_for_memory_request(cores_in_mcpu, memory_in_bytes, worker_type):
    min_cores_mcpu = memory_bytes_to_cores_mcpu(memory_in_bytes, worker_type)
    return max(cores_in_mcpu, min_cores_mcpu)


def total_worker_storage_gib():
    # local ssd is 375Gi
    # reserve 25Gi for images
    return 375 - 25


def worker_storage_per_core_bytes(worker_cores):
    return (total_worker_storage_gib() * 1024**3) // worker_cores


def storage_bytes_to_cores_mcpu(storage_in_bytes, worker_cores):
    return round_up_division(storage_in_bytes * 1000, worker_storage_per_core_bytes(worker_cores))


def cores_mcpu_to_storage_bytes(cores_in_mcpu, worker_cores):
    return (cores_in_mcpu * worker_storage_per_core_bytes(worker_cores)) // 1000


def adjust_cores_for_storage_request(cores_in_mcpu, storage_in_bytes, worker_cores):
    min_cores_mcpu = storage_bytes_to_cores_mcpu(storage_in_bytes, worker_cores)
    return max(cores_in_mcpu, min_cores_mcpu)


def adjust_cores_for_packability(cores_in_mcpu):
    cores_in_mcpu = max(1, cores_in_mcpu)
    power = max(-2, math.ceil(math.log2(cores_in_mcpu / 1000)))
    return int(2**power * 1000)


image_regex = re.compile(r"(?:.+/)?([^:]+)(:(.+))?")


def parse_image_tag(image_string):
    match = image_regex.fullmatch(image_string)
    if match:
        return match.group(3)
    return None
