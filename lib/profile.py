import time


def profile(f, *args, **kwargs):
    """
    Execute f and return the result along with the time in seconds.
    """
    now = time.perf_counter()

    # execute and determine how long it took
    return f(*args, **kwargs), time.perf_counter() - now
