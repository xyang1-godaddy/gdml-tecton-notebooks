import logging
import time

from botocore.exceptions import ClientError
from botocore.exceptions import HTTPClientError
from botocore.vendored.requests.packages.urllib3.exceptions import HTTPError as HTTPError1
from urllib3.exceptions import HTTPError as HTTPError2

log = logging.getLogger("retryloop")


def check_retryable_error(boto_error, attempt, retryable_errors=None):
    """
    Checks the ClientError exception for retryable error codes. If retryable,
    sleep for 60 seconds and return True. Otherwise return False.
    :param boto_error: a boto ClientError
    :param attempt: the retry attempt number
    :param retryable_errors: tuple of error codes, or None if all errors are to be retried
    """
    msg = boto_error.response["Error"]["Code"]
    # https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
    if retryable_errors and msg not in retryable_errors:
        return False
    log.info("Retrying request due to ClientError %s (attempt %s)", boto_error, attempt)
    # there is already backoff inside of boto, no need to do it here
    time.sleep(60)
    return True


def retry_loop(retryable_errors=None, retry_average_fn=lambda: 0):
    """
    Retries an arbitrary function by catching and checking the ClientError
    to see if it should be retried. This retries until the function succeeds,
    check_retryable_error returns false or the maximum number of attempts is
    reached, whichever occurs first.
    :param retryable_errors: tuple of error codes, or None if all errors are to be retried
    :retry_average: a retry counter function to be called once per retry
    """

    def _decorator_retry(function):
        """
        :param function: a function to repeat
        """

        def _retry_loop(*args, attempts=10, **kwargs):
            """
            :param *args: args to pass to function
            :param attempts: the maximum number of times to try calling the function
            :param **kwargs: kwargs to pass to function
            """
            for attempt in range(attempts - 1):
                try:
                    return function(*args, **kwargs)
                except ClientError as e:
                    if not check_retryable_error(e, attempt, retryable_errors):
                        raise
                except (HTTPError1, HTTPError2, HTTPClientError) as e:
                    log.info("Retrying request due to HTTPError %s (attempt %s)", e, attempt)
                retry_average_fn()
            # no catch on the final attempt
            return function(*args, **kwargs)

        return _retry_loop

    return _decorator_retry
