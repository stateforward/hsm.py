import os
import time
import socket
import hashlib
from typing import Optional

# Configuration
TIMESTAMP_BITS = 41
MACHINE_ID_BITS = 14
COUNTER_BITS = 9

# Shifts
TIMESTAMP_SHIFT = MACHINE_ID_BITS + COUNTER_BITS
MACHINE_ID_SHIFT = COUNTER_BITS

# Masks
MAX_MACHINE_ID = (1 << MACHINE_ID_BITS) - 1
MAX_COUNTER = (1 << COUNTER_BITS) - 1

# Epoch (same as TS: 1700000000000)
EPOCH = 1700000000000


class MUIDGenerator:
    def __init__(self, machine_id: Optional[int] = None):
        if machine_id is None:
            machine_id = self._generate_machine_id()
        self.machine_id = machine_id & MAX_MACHINE_ID
        self.last_timestamp = -1
        self.counter = 0

    def _generate_machine_id(self) -> int:
        try:
            hostname = socket.gethostname().encode("utf-8")
            return int(hashlib.md5(hostname).hexdigest(), 16) & MAX_MACHINE_ID
        except:
            return int.from_bytes(os.urandom(2), "big") & MAX_MACHINE_ID

    def make(self) -> str:
        current_timestamp = (time.time_ns() // 1_000_000) - EPOCH

        if current_timestamp < self.last_timestamp:
            # Clock moved backwards, reuse last timestamp
            current_timestamp = self.last_timestamp

        if current_timestamp == self.last_timestamp:
            self.counter = (self.counter + 1) & MAX_COUNTER
            if self.counter == 0:
                # Overflow, wait for next ms
                while current_timestamp == self.last_timestamp:
                    current_timestamp = (time.time_ns() // 1_000_000) - EPOCH
        else:
            self.counter = 0

        self.last_timestamp = current_timestamp

        # Compose ID
        muid_int = (
            (current_timestamp << TIMESTAMP_SHIFT)
            | (self.machine_id << MACHINE_ID_SHIFT)
            | self.counter
        )

        # Inlined base32 conversion
        num = muid_int
        if num == 0:
            return "0"

        # Use simple string indexing.
        # Making ALPHABET global or class var might help slightly but string literal is fast enough via intern.
        alphabet = "0123456789abcdefghijklmnopqrstuv"
        res = []
        while num:
            res.append(alphabet[num & 31])
            num >>= 5
        return "".join(reversed(res))


_global_generator = MUIDGenerator()


def make() -> str:
    return _global_generator.make()
