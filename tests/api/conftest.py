import os

os.environ.setdefault("BIOINDEX_TOKEN_SIGNING_KEY", "00" * 32)  # 32 bytes (hex)

# the generation is normally read from the database; pin it so continuation
# tokens minted and resumed in these tests agree
import bioindex.api.bio as _bio  # noqa: E402
_bio.index_generation = lambda engine, name, ttl=30: "gen-test-fixed"
