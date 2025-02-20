import time
from asyncio import Semaphore
from typing import Optional


class RateLimiter:
    def __init__(self, tokens_per_min: int = 10000, requests_per_min: int = 30):
        self.tokens_per_min = tokens_per_min
        self.requests_per_min = requests_per_min
        self.tokens_used = 0
        self.requests_made = 0
        self.last_reset = time.time()
        self.semaphore = Semaphore(3)  # Max concurrent requests

    async def acquire(self, tokens_needed: Optional[int] = None):
        async with self.semaphore:
            current_time = time.time()
            if current_time - self.last_reset >= 60:
                self.tokens_used = 0
                self.requests_made = 0
                self.last_reset = current_time

            if tokens_needed and self.tokens_used + tokens_needed > self.tokens_per_min:
                wait_time = 60 - (current_time - self.last_reset)
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                self.tokens_used = 0
                self.requests_made = 0
                self.last_reset = time.time()

            if self.requests_made >= self.requests_per_min:
                wait_time = 60 - (current_time - self.last_reset)
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                self.tokens_used = 0
                self.requests_made = 0
                self.last_reset = time.time()

            if tokens_needed:
                self.tokens_used += tokens_needed
            self.requests_made += 1
