from pathlib import Path

from benchmark_context import BenchmarkContext
from runners import BaseRunner


class ImporterPokec:
    def __init__(
        self, benchmark_context: BenchmarkContext, dataset_name: str, variant: str, index_file: str, dataset_file: str
    ) -> None:
        self._benchmark_context = benchmark_context
        self._dataset_name = dataset_name
        self._variant = variant
        self._index_file = index_file
        self._dataset_file = dataset_file

    def execute_import(self):
        if self._benchmark_context.vendor_name == "neo4j":

            vendor_runner = BaseRunner.create(
                benchmark_context=self._benchmark_context,
            )
            client = vendor_runner.fetch_client()
            vendor_runner.clean_db()
            vendor_runner.start_preparation("preparation")
            print("Executing database cleanup and index setup...")
            client.execute(file_path=self._index_file, num_workers=1)
            vendor_runner.stop("preparation")
            neo4j_dump = Path() / ".cache" / "datasets" / self._dataset_name / self._variant / "neo4j.dump"
            if neo4j_dump.exists():
                vendor_runner.load_db_from_dump(path=neo4j_dump.parent)
            else:
                vendor_runner.start_preparation("import")
                print("Importing dataset...")
                client.execute(file_path=self._dataset_file, num_workers=self._benchmark_context.num_workers_for_import)
                vendor_runner.stop("import")
                vendor_runner.dump_db(path=neo4j_dump.parent)

            return True
        else:
            return False
