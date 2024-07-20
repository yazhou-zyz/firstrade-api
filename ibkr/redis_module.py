import json
from redis import ConnectionPool, Redis

class RedisClient:
    def __init__(self, host='localhost', port=6379, db=0):
        self.pool = ConnectionPool(host=host, port=port, db=db)
        self.client = Redis(connection_pool=self.pool)

    def set(self, key, value, max_retries=3):
        for attempt in range(max_retries):
            try:
                self.client.set(key, json.dumps(value))
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e

    def get(self, key):
        try:
            value = self.client.get(key)
            if value:
                return json.loads(value)
            return None
        except json.JSONDecodeError as e:
            raise e
        except Exception as e:
            raise e
