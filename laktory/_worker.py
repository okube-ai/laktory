import subprocess


class Worker:

    def run(self, cmd, cwd=None):
        try:
            completed_process = subprocess.run(
                cmd,
                cwd=cwd,
                check=True,
            )

        except Exception as e:
            print("An error occurred:", str(e))
