from pathlib import Path


class Paths:
    def __init__(self, file):
        self.root = Path(file).parent
        self.data = self.root / "data"
        self.tmp = self.root / "tmp"

        if not self.tmp.exists():
            self.tmp.mkdir(parents=True, exist_ok=True)
