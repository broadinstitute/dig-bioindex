import dataclasses
import threading
import time
import typing

import lib.reader
import lib.utils


_cont_map = {}
_cont_lock = threading.RLock()


@dataclasses.dataclass()
class Cont:
    callback: any
    expiration: float = None

    def __post_init__(self):
        """
        Set the default expiration.
        """
        self.expiration = time.time() + 60


def make_continuation(**kwargs):
    """
    Create a continuation and return a token to it.
    """
    cont = Cont(**kwargs)
    token = lib.utils.nonce()

    # add it to the map
    with _cont_lock:
        _cont_map[token] = cont

    return token


def lookup_continuation(token):
    """
    Return a continuation from its token.
    """
    with _cont_lock:
        return _cont_map[token]


def remove_continuation(token):
    """
    Remove a continuation token from the map.
    """
    with _cont_lock:
        del _cont_map[token]


def cleanup_continuations():
    """
    Runs forever in the background, every minute it will remove any expired
    continues from the map.
    """
    while True:
        time.sleep(60)

        with _cont_lock:
            now = time.time()
            tokens = list(_cont_map.keys())

            # remove all expired continuations
            for token in tokens:
                if now > _cont_map[token].expiration:
                    del _cont_map[token]


# Spin up a thread that periodically removes old continuations.
threading.Thread(target=cleanup_continuations, daemon=True). \
    start()
