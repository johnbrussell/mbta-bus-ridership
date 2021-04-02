import unittest
from collections import namedtuple

from mbta_bus_ridership.csv_reader import CsvReader, dataWithColumns

MbtaData = namedtuple('MbtaData', ['col1', 'col2', 'col3'])


class TestCsvReader(unittest.TestCase):
    def test_read(self):
        subject = CsvReader()
        actual = subject.read('./test/resources/read_csv.csv')
        expected_data = [
            MbtaData(col1='data1', col2='data2', col3='data3'),
            MbtaData(col1='data1 line 2', col2='data2', col3=''),
            MbtaData(col1='', col2='2.5', col3='3'),
        ]
        expected_columns = {'col1': 0, 'col2': 1, 'col3': 2}
        expected = dataWithColumns(data=expected_data, columns=expected_columns)
        self.assertEqual(actual, expected)

        # Test that accessing a column does not generate an index out of range error
        for datum in actual.data:
            for column in actual.columns.keys():
                datum[actual.columns[column]]
