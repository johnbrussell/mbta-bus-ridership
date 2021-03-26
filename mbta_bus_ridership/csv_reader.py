import csv
from collections import namedtuple

dataWithColumns = namedtuple('dataWithColumns', ['data', 'columns'])


class CsvReader:
    def read(self, file_path, delimiter=','):
        with open(file_path) as f:
            csv_reader = csv.reader(f, delimiter=delimiter)
            columns = next(csv_reader)
            unread_data = dataWithColumns(data=csv_reader, columns=self._create_csv_column_dict(columns))
            data = self._read_in_data(unread_data)
        return data

    @staticmethod
    def _create_csv_column_dict(columns):
        # Takes the first row of a CSV file (ie., the columns).  Returns dict(column_name: index_in_row).
        column_index = 0
        column_indices = dict()
        for name in columns:
            if name:
                column_indices[name] = column_index
            column_index += 1
        return column_indices

    @staticmethod
    def _read_in_data(data_with_columns):
        columns = data_with_columns.columns
        data_structure = namedtuple('MbtaData', [str(k) for k in columns.keys() if str(k)])
        return dataWithColumns([data_structure(**{col: line[columns[col]] for col in columns.keys() if col})
                                for line in data_with_columns.data], columns)
