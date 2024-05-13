import datetime as date
import glob
from pathlib import Path
import os


class PromClass:
    """
    A class for formatting and writing logs which are readible by Prometheus
    monitoring software. Currently this only logs a 'job completed' message.
    """
    def __init__(
        self,
        jobname: str,
    ):
        self.out_path = os.environ.get(
            "PROM_PATH",
        )
        # make sure jobname matches Prom data model (no hyphens)
        self.jobname = jobname
        self.metrics = []
        self.ppid = os.getppid()

    def format_metrics(
            self
    ) -> None:
        """ 
        Formats a Prometheus-compatible 'job completed' metric.
        Adds it to a list of ready-to-write metrics.
        """
        timestamp = int(round(date.datetime.now().timestamp()))
        completed_metric = f"{self.jobname}_completed {timestamp}"
        self.metrics.append(completed_metric)

    def emit_metrics(
            self
    ) -> None:
        """
        Saves the Prometheus metrics to an output prom file.
        Handles the deletion of older metrics ending in *.prom, to prevent
        interference with logging.
        """
        # write metric to a temporary path
        temp_filename = f"{self.out_path}/{self.jobname}.prom.{self.ppid}"

        with open(temp_filename, "a") as new_file:
            for metric in self.metrics:
                new_file.write(metric + "\n")

        old_files = glob.glob(f"{self.out_path}/{self.jobname}*.prom")
        if old_files:
            for file in old_files:
                Path(file).unlink()

        # rename the new metric file and set permissions
        new_filename = Path(f"{self.out_path}/{self.jobname}.prom")
        Path(temp_filename).rename(new_filename)
        os.chmod(new_filename, int("644", base=8))

