import csv
from collections import namedtuple

dataWithColumns = namedtuple('dataWithColumns', ['data', 'columns'])


class CsvReader:
    def read(self, file_path, delimiter=','):
        with open(file_path) as f:
            csv_reader = csv.reader(f, delimiter=delimiter)
            columns = next(csv_reader)
            data = dataWithColumns(data=csv_reader, columns=self._create_csv_column_dict(columns))
        return data

    @staticmethod
    def _create_csv_column_dict(columns):
        # Takes the first row of a CSV file (ie., the columns).  Returns dict(column_name: index_in_row).
        column_index = 0
        column_indices = dict()
        for name in columns:
            column_indices[name] = column_index
            column_index += 1
        return column_indices
