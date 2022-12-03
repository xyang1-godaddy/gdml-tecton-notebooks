import functools
import logging
import time


def periodic_logging(generator=None, log_interval_s=60, log=logging.getLogger(__name__)):
    if generator is None:
        return functools.partial(periodic_logging, log_interval_s=log_interval_s, log=log)

    @functools.wraps(generator)
    def wrapper(*args, **kwargs):
        last_log_time = time.time()
        for i, value in enumerate(generator(*args, **kwargs)):
            if time.time() - last_log_time > log_interval_s:
                log.info("Finished iterating over {} items".format(i))
                last_log_time = time.time()
            yield value

    return wrapper
