import dataclasses
import msgpack


@dataclasses.dataclass(frozen=True)
class Record:
    """
    File position and length of a record in a table.
    """

    # the id of the redis table (s3 location)
    table_id: int

    # the byte range in the table of the record
    offset: int
    length: int

    @staticmethod
    def unpack(msg):
        """
        Unpack redis bytes.
        """
        return Record(*msgpack.loads(msg))

    def pack(self):
        """
        Pack this record into a message for redis.
        """
        return msgpack.dumps((self.table_id, self.offset, self.length))
