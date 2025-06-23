import os


class MonkeyPatch:
    def __init__(self):
        self.env0 = {}

    def setenv(self, key, value):
        self.env0[key] = os.getenv(key, None)
        os.environ[key] = value

    def cleanup(self):
        for k, v in self.env0.items():
            if v is None:
                del os.environ[k]
            else:
                os.environ[k] = v
