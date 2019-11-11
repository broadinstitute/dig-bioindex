import dataclasses
import threading
import time
import secrets


_cont_map = {}
_cont_lock = threading.RLock()


@dataclasses.dataclass()
class Cont:
    results: any
    key: str
    locus: str
    expiration: float = None

    def __post_init__(self):
        """
        Set the default expiration to 2 minutes from now.
        """
        self.update()

    def update(self):
        """
        Keep the continuation alive for another minute.
        """
        self.expiration = time.time() + 60


def make_continuation(**kwargs):
    """
    Create a continuation and return a token to it.
    """
    cont = Cont(**kwargs)
    token = secrets.token_urlsafe()

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
