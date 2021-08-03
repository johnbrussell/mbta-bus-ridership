import csv


class DataWriter:
    @staticmethod
    def write_csv(data, columns, filename):
        with open(filename, 'w') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(columns.keys())
            csv_writer.writerows(data)
