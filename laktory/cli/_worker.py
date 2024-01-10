import subprocess


class Worker:
    def run(self, cmd, cwd=None, raise_exceptions=True):
        try:
            completed_process = subprocess.run(
                cmd,
                cwd=cwd,
                check=True,
            )

        except Exception as e:
            if raise_exceptions:
                raise e
            else:
                print("An error occurred:", str(e))
