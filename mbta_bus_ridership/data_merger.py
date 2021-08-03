from mbta_bus_ridership.csv_reader import dataWithColumns
from collections import namedtuple


WEEKDAY_SERVICE_ID_KEYWORDS = ['Weekday', '-Wdy-']
SATURDAY_SERVICE_ID_KEYWORDS = ['Saturday', '-Sa-']
SUNDAY_SERVICE_ID_KEYWORDS = ['Sunday', '-Su-']
INVALID_SERVICE_ID_PREFIXES = ['RTL', 'Boat', 'CR', 'LRV']
INVALID_SERVICE_IDS = ['BUS419-10-Wdy-02']


class DataMerger:
    def __init__(self):
        self._stations_not_in_gtfs_data = set()

    def merge(self, ridership_csv_data, gtfs_data):
        ridership_csv_data = self._add_lat_long_to_data(ridership_csv_data, gtfs_data)
        ridership_csv_data = self._add_route_and_trip_ids_to_data(ridership_csv_data, gtfs_data)
        return self._add_stop_departure_time(ridership_csv_data, gtfs_data)

    def _add_lat_long_to_data(self, ridership_csv_data, gtfs_data):
        col_names = sorted(ridership_csv_data.columns.keys(), key=lambda x: ridership_csv_data.columns[x])  # ascending
        old_col_names = col_names.copy()
        col_names.extend(['lat', 'long'])
        new_columns = {col_names[i]: i for i in range(len(col_names))}
        new_data_structure = namedtuple('MbtaData', col_names)
        new_data = dataWithColumns(
            data=[self._add_lat_long_to_line(
                     line, ridership_csv_data.columns, old_col_names, new_data_structure, gtfs_data)
                  for line in ridership_csv_data.data],
            columns=new_columns
        )
        print("stations not in gtfs data:", self._stations_not_in_gtfs_data)
        print("num stations not in gtfs data; num stations", len(self._stations_not_in_gtfs_data),
              len({line[new_columns['stop_id']] for line in ridership_csv_data.data}))
        return new_data

    def _add_lat_long_to_line(self, old_data_line, old_columns, old_column_names, new_data_structure, gtfs_data):
        data_dict = {col: old_data_line[old_columns[col]] for col in old_column_names}
        stop_id = old_data_line[old_columns['stop_id']]
        if stop_id not in gtfs_data.stopLocations:
            self._stations_not_in_gtfs_data.add(stop_id)
            return new_data_structure(lat=None, long=None, **data_dict)
        stop_info = gtfs_data.stopLocations[stop_id]
        return new_data_structure(lat=stop_info.lat, long=stop_info.long, **data_dict)

    def _add_route_and_trip_ids_to_data(self, ridership_csv_data, gtfs_data):
        col_names = sorted(ridership_csv_data.columns.keys(), key=lambda x: ridership_csv_data.columns[x])  # ascending
        old_col_names = col_names.copy()
        col_names.extend(['gtfs_route_id', 'gtfs_trip_id'])
        new_columns = {col_names[i]: i for i in range(len(col_names))}
        new_data_structure = namedtuple('MbtaData', col_names)
        new_data_with_origin_info = []
        trip_start_line = None
        for line in ridership_csv_data.data:
            if trip_start_line is None or \
                    trip_start_line[new_columns['route_variant']] != line[new_columns['route_variant']] or \
                    trip_start_line[new_columns['trip_start_time']] != line[new_columns['trip_start_time']] or \
                    trip_start_line[new_columns['day_type_name']] != line[new_columns['day_type_name']]:
                trip_start_line = line
                new_line = self._add_route_id_and_trip_id_to_trip_origin_line(line, ridership_csv_data.columns,
                                                                              old_col_names, new_data_structure,
                                                                              gtfs_data, ridership_csv_data.data)
            else:
                data_dict = {col: line[ridership_csv_data.columns[col]] for col in old_col_names}
                new_line = new_data_structure(gtfs_route_id=None, gtfs_trip_id=None, **data_dict)
            new_data_with_origin_info.append(new_line)
        del ridership_csv_data
        new_data = dataWithColumns(data=[], columns=new_columns)
        trip_start_line = None
        while len(new_data_with_origin_info) > 0:
            new_line = new_data_with_origin_info.pop(0)
            if trip_start_line is None or \
                    trip_start_line[new_columns['route_variant']] != new_line[new_columns['route_variant']] or \
                    trip_start_line[new_columns['trip_start_time']] != new_line[new_columns['trip_start_time']] or \
                    trip_start_line[new_columns['day_type_name']] != new_line[new_columns['day_type_name']]:
                trip_start_line = new_line
            new_line = new_line._replace(gtfs_route_id=trip_start_line.gtfs_route_id,
                                         gtfs_trip_id=trip_start_line.gtfs_trip_id)
            new_data.data.append(new_line)
        return new_data

    def _add_route_id_and_trip_id_to_trip_origin_line(self, old_data_line, old_columns, old_col_names,
                                                      new_data_structure, gtfs_data, all_ridership_data,
                                                      original_route=None):
        data_dict = {col: old_data_line[old_columns[col]] for col in old_col_names}
        if original_route:
            data_dict['route_id'] = original_route
        relevant_gtfs_trips = {t for t, t_sched in gtfs_data.tripSchedules.items() if
                               t_sched.tripRouteInfo.routeId == old_data_line[old_columns['route_id']] and
                               self._get_day_from_service_id(t_sched.serviceId) ==
                               old_data_line[old_columns['day_type_name']] and
                               t_sched.serviceId not in INVALID_SERVICE_IDS and
                               t_sched.tripStops[min(t_sched.tripStops.keys(), key=lambda x: int(x))].stopId ==
                               old_data_line[old_columns['stop_id']] and
                               t_sched.tripStops[min(t_sched.tripStops.keys(), key=lambda x: int(x))].departureTime ==
                               old_data_line[old_columns['trip_start_time']]
                               }
        if len(relevant_gtfs_trips) == 0:
            alt_data_line = self._create_alternative_ridership_data_line(old_data_line, old_columns)
            if not alt_data_line:
                return new_data_structure(gtfs_route_id=None, gtfs_trip_id=None, **data_dict)
            if not original_route:
                original_route = old_data_line[old_columns['route_id']]
            new_old_data_line = self._add_route_id_and_trip_id_to_trip_origin_line(
                alt_data_line, old_columns, old_col_names, new_data_structure, gtfs_data, all_ridership_data,
                original_route
            )
            return new_old_data_line if new_old_data_line.gtfs_trip_id is not None else \
                new_data_structure(gtfs_route_id=None, gtfs_trip_id=None, **data_dict)
        if len(relevant_gtfs_trips) > 1:
            trip_ridership_data = [d for d in all_ridership_data if
                                   d[old_columns['route_variant']] == old_data_line[old_columns['route_variant']] and
                                   d[old_columns['day_type_name']] == old_data_line[old_columns['day_type_name']] and
                                   d[old_columns['trip_start_time']] == old_data_line[old_columns['trip_start_time']]
                                   ]
            last_stop_line = max(trip_ridership_data, key=lambda line: int(line[old_columns['stop_sequence']]))
            relevant_gtfs_trips = {t for t in relevant_gtfs_trips if
                                   gtfs_data.tripSchedules[t].tripStops[
                                       max(gtfs_data.tripSchedules[t].tripStops.keys(), key=lambda x: int(x))].stopId ==
                                   last_stop_line[old_columns['stop_id']] and
                                   len(trip_ridership_data) == len(gtfs_data.tripSchedules[t].tripStops)
                                   }
            if len(relevant_gtfs_trips) != 1:
                print(len(relevant_gtfs_trips), "still broken", old_data_line,
                      {trip: gtfs_data.tripSchedules[trip] for trip in relevant_gtfs_trips})
                return new_data_structure(gtfs_route_id=None, gtfs_trip_id=None, **data_dict)

        trip = relevant_gtfs_trips.pop()
        route = None
        for route_number, trips_info in gtfs_data.uniqueRouteTrips.items():
            if trip in trips_info.tripIds:
                route = route_number
                break
        assert(route is not None)
        return new_data_structure(
            gtfs_route_id=route,
            gtfs_trip_id=trip,
            **data_dict
        )

    def _add_stop_departure_time(self, ridership_csv_data, gtfs_data):
        col_names = sorted(ridership_csv_data.columns.keys(), key=lambda x: ridership_csv_data.columns[x])  # ascending
        old_col_names = col_names.copy()
        col_names.append('departure_time')
        new_columns = {col_names[i]: i for i in range(len(col_names))}
        new_data_structure = namedtuple('MbtaData', col_names)
        new_data_with_departure_time = []
        for line in ridership_csv_data.data:
            new_data_with_departure_time.append(self._add_stop_departure_time_to_line(line, ridership_csv_data.columns,
                                                                                      old_col_names, new_data_structure,
                                                                                      gtfs_data))
        return dataWithColumns(data=new_data_with_departure_time, columns=new_columns)

    @staticmethod
    def _add_stop_departure_time_to_line(old_data_line, old_columns, old_col_names, new_data_structure, gtfs_data):
        data_dict = {col: old_data_line[old_columns[col]] for col in old_col_names}
        if old_data_line[old_columns['gtfs_trip_id']] is None:
            return new_data_structure(departure_time=None, **data_dict)
        gtfs_trip = gtfs_data.tripSchedules[old_data_line[old_columns['gtfs_trip_id']]]
        if old_data_line[old_columns['stop_sequence']] in gtfs_trip.tripStops and \
                gtfs_trip.tripStops[old_data_line[old_columns['stop_sequence']]].stopId == \
                old_data_line[old_columns['stop_id']]:
            return new_data_structure(
                departure_time=gtfs_trip.tripStops[old_data_line[old_columns['stop_sequence']]].departureTime,
                **data_dict
            )
        if not any(stop.stopId == old_data_line[old_columns['stop_id']] for stop in gtfs_trip.tripStops.values()):
            return new_data_structure(departure_time=None, **data_dict)
        for key in sorted(gtfs_trip.tripStops.keys(), key=lambda x: int(x)):
            stop = gtfs_trip.tripStops[key]
            if stop.stopId == old_data_line[old_columns['stop_id']]:
                return new_data_structure(departure_time=stop.departureTime, **data_dict)
        return new_data_structure(departure_time=None, **data_dict)

    @staticmethod
    def _create_alternative_ridership_data_line(old_data_line, columns):
        data_dict = {col: old_data_line[idx] for col, idx in columns.items()}
        route_conversion_dict = {
            '19': '201',
            '21': '57',
            '23': '57',
            '24': '2427',
            '27': '2427',
            '2427': '36',
            '32': '3233',
            '33': '32',
            '3233': '57',
            '34': '57',
            '36': '57',
            '37': '3738',
            '38': '3738',
            '3738': '57',
            '40': '4050',
            '57': '57A',
            '57A': '39',
            '62': '627',
            '66': '23',
            '89': '8993',
            '93': '92',
            '116': '116117',
            '216': '214216',
            '442': '441442',
        }
        stop_conversion_dict = {
            '7774': '7783',
            '74611': '74617',
            '74617': '9074611',
        }
        if data_dict['route_id'] not in route_conversion_dict and data_dict['stop_id'] not in stop_conversion_dict:
            return None
        data_dict['route_id'] = route_conversion_dict.get(data_dict['route_id'], data_dict['route_id'])
        data_dict['stop_id'] = stop_conversion_dict.get(data_dict['stop_id'], data_dict['stop_id'])

        col_names = sorted(columns.keys(), key=lambda x: columns[x])  # ascending
        new_data_structure = namedtuple('MbtaData', col_names)
        return new_data_structure(**data_dict)

    @staticmethod
    def _get_day_from_service_id(service_id):
        if any(k in service_id for k in WEEKDAY_SERVICE_ID_KEYWORDS):
            return 'weekday'
        if any(k in service_id for k in SATURDAY_SERVICE_ID_KEYWORDS):
            return 'saturday'
        if any(k in service_id for k in SUNDAY_SERVICE_ID_KEYWORDS):
            return 'sunday'
