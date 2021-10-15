import csv

from click.testing import CliRunner
from datetime import datetime
from os.path import join
from tempfile import TemporaryDirectory

from tarentula.cli import cli
from tests.test_abstract import TestAbstract


class TestAggregate(TestAbstract):
    def tearDown(self):
        super().tearDown()

    def test_agg1(self):
        with self.existing_species_documents(), TemporaryDirectory() as tmp:
            output_file = join(tmp, 'output.csv')

            runner = CliRunner()
            runner.invoke(cli, ['aggregate', '--datashare-url', self.datashare_url, '--elasticsearch-url',
                                self.elasticsearch_url, '--datashare-project', self.datashare_project, '--output-file', output_file])

            with open(output_file, newline='') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                # {'key': 'audio/vorbis', 'doc_count': '2'}

                # Header
                csv_reader.fieldnames
                self.assertIn('key', csv_reader.fieldnames)

                # First row
                row = next(csv_reader)
                self.assertIn('key', row)
                self.assertGreaterEqual(len(row), 2)