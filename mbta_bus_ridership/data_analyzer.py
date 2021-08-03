from mbta_bus_ridership.csv_reader import dataWithColumns
from collections import namedtuple
import math


class DataAnalyzer:
    def aggregate_frequent_route_data(self, csv_data, any_day=False):
        new_column_names = ['route_name', 'gtfs_route_id', 'route_variant', 'day_type_name', 'total_trips',
                            'total_boardings', 'total_alightings', 'total_on_off', 'total_svc_minutes',
                            'total_svc_layover_minutes', 'total_miles', 'total_marginal_mins', 'total_marginal_miles',
                            'alightings_per_trip', 'alightings_per_svc_minute', 'alightings_per_svc_layover_minute',
                            'alightings_per_mile', 'alightings_per_marginal_min', 'alightings_per_marginal_mile',
                            'boardings_per_trip', 'boardings_per_svc_minute', 'boardings_per_svc_layover_minute',
                            'boardings_per_mile', 'boardings_per_marginal_min',  'boardings_per_marginal_mile',
                            'on_off_per_trip', 'on_off_per_svc_minute', 'on_off_per_svc_layover_minute',
                            'on_off_per_mile', 'on_off_per_marginal_min', 'on_off_per_marginal_mile',
                            'avg_max_load', 'avg_load_busiest_pair', 'pct_of_route_service']
        new_columns = dict()
        for i in range(len(new_column_names)):
            new_columns[new_column_names[i]] = i

        all_routes = set(line[csv_data.columns['gtfs_route_id']] for line in csv_data.data)

        route_data = []
        for route in all_routes:
            route_data.extend(self._aggregate_route(route, new_column_names, csv_data, any_day))

        route_data = [line for line in route_data if line[new_columns['total_trips']] >= 6]

        data = dataWithColumns(data=route_data, columns=new_columns)
        data = self._add_rank('alightings_per_trip', data.data, data.columns)
        data = self._add_rank('alightings_per_svc_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('alightings_per_svc_layover_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('alightings_per_mile', data.data, data.columns)
        data = self._add_rank('alightings_per_marginal_min', data.data, data.columns, description_precision=2)
        data = self._add_rank('alightings_per_marginal_mile', data.data, data.columns, description_precision=2)
        data = self._add_rank('boardings_per_trip', data.data, data.columns)
        data = self._add_rank('boardings_per_svc_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('boardings_per_svc_layover_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('boardings_per_mile', data.data, data.columns)
        data = self._add_rank('boardings_per_marginal_min', data.data, data.columns, description_precision=2)
        data = self._add_rank('boardings_per_marginal_mile', data.data, data.columns, description_precision=2)
        data = self._add_rank('on_off_per_trip', data.data, data.columns)
        data = self._add_rank('on_off_per_svc_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('on_off_per_svc_layover_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('on_off_per_mile', data.data, data.columns)
        data = self._add_rank('on_off_per_marginal_min', data.data, data.columns, description_precision=2)
        data = self._add_rank('on_off_per_marginal_mile', data.data, data.columns, description_precision=2)
        data = self._add_rank('avg_max_load', data.data, data.columns)
        data = self._add_rank('avg_load_busiest_pair', data.data, data.columns)
        return data

    def aggregate_route_data(self, csv_data, any_day=False):
        new_column_names = ['route_name', 'gtfs_route_id', 'route_variant', 'day_type_name', 'total_trips',
                            'total_boardings', 'total_alightings', 'total_on_off', 'total_svc_minutes',
                            'total_svc_layover_minutes', 'total_miles', 'total_marginal_mins', 'total_marginal_miles',
                            'alightings_per_trip', 'alightings_per_svc_minute', 'alightings_per_svc_layover_minute',
                            'alightings_per_mile', 'alightings_per_marginal_min', 'alightings_per_marginal_mile',
                            'boardings_per_trip', 'boardings_per_svc_minute', 'boardings_per_svc_layover_minute',
                            'boardings_per_mile', 'boardings_per_marginal_min', 'boardings_per_marginal_mile',
                            'on_off_per_trip', 'on_off_per_svc_minute', 'on_off_per_svc_layover_minute',
                            'on_off_per_mile', 'on_off_per_marginal_min', 'on_off_per_marginal_mile', 'avg_max_load',
                            'avg_load_busiest_pair', 'pct_of_route_service']
        new_columns = dict()
        for i in range(len(new_column_names)):
            new_columns[new_column_names[i]] = i

        all_routes = set(line[csv_data.columns['gtfs_route_id']] for line in csv_data.data)

        route_data = []
        for route in all_routes:
            route_data.extend(self._aggregate_route(route, new_column_names, csv_data, any_day))
        data = dataWithColumns(data=route_data, columns=new_columns)
        data = self._add_rank('alightings_per_trip', data.data, data.columns)
        data = self._add_rank('alightings_per_svc_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('alightings_per_svc_layover_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('alightings_per_mile', data.data, data.columns)
        data = self._add_rank('alightings_per_marginal_min', data.data, data.columns, description_precision=2)
        data = self._add_rank('alightings_per_marginal_mile', data.data, data.columns, description_precision=2)
        data = self._add_rank('boardings_per_trip', data.data, data.columns)
        data = self._add_rank('boardings_per_svc_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('boardings_per_svc_layover_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('boardings_per_mile', data.data, data.columns)
        data = self._add_rank('boardings_per_marginal_min', data.data, data.columns, description_precision=2)
        data = self._add_rank('boardings_per_marginal_mile', data.data, data.columns, description_precision=2)
        data = self._add_rank('on_off_per_trip', data.data, data.columns)
        data = self._add_rank('on_off_per_svc_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('on_off_per_svc_layover_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('on_off_per_mile', data.data, data.columns)
        data = self._add_rank('on_off_per_marginal_min', data.data, data.columns, description_precision=2)
        data = self._add_rank('on_off_per_marginal_mile', data.data, data.columns, description_precision=2)
        data = self._add_rank('avg_max_load', data.data, data.columns)
        data = self._add_rank('avg_load_busiest_pair', data.data, data.columns)
        return data

    def aggregate_route_day_data(self, stop_trend_data):
        output_columns = ['route_name', 'total_weekday_on_off', 'total_saturday_on_off',
                          'total_sunday_on_off', 'total_weekday_trips', 'total_saturday_trips', 'total_sunday_trips',
                          'weekday_on_off', 'saturday_on_off', 'sunday_on_off', 'total_sat_pct', 'total_sun_pct',
                          'sat_pct', 'sun_pct']

        all_routes = set(line[stop_trend_data.columns['route_name']] for line in stop_trend_data.data)

        data_structure = namedtuple('MbtaData', output_columns)
        output_data = list()
        for route in all_routes:
            route_stop_data = [line for line in stop_trend_data.data if
                               line[stop_trend_data.columns['route_name']] == route]
            directions = set([line[stop_trend_data.columns['direction']] for line in route_stop_data])
            weekday_on_off = sum(line[stop_trend_data.columns['total_weekday_on_off']] for line in route_stop_data)
            num_weekday_trips = sum([max(line[stop_trend_data.columns['total_weekday_trips']] for line in
                                    [ln for ln in route_stop_data if ln[stop_trend_data.columns['direction']] == d])
                                     for d in directions])
            saturday_on_off = sum(line[stop_trend_data.columns['total_saturday_on_off']] for line in route_stop_data)
            num_saturday_trips = sum([max(line[stop_trend_data.columns['total_saturday_trips']] for line in
                                     [ln for ln in route_stop_data if ln[stop_trend_data.columns['direction']] == d])
                                     for d in directions])
            sunday_on_off = sum(line[stop_trend_data.columns['total_sunday_on_off']] for line in route_stop_data)
            num_sunday_trips = sum([max(line[stop_trend_data.columns['total_sunday_trips']] for line in
                                    [ln for ln in route_stop_data if ln[stop_trend_data.columns['direction']] == d])
                                    for d in directions])
            weekday_per_trip = weekday_on_off / num_weekday_trips if num_weekday_trips > 0 else None
            saturday_per_trip = saturday_on_off / num_saturday_trips if num_saturday_trips > 0 else None
            sunday_per_trip = sunday_on_off / num_sunday_trips if num_sunday_trips > 0 else None
            total_sat_pct = saturday_on_off / weekday_on_off if \
                weekday_on_off > 0 and saturday_on_off > 0 else None
            total_sun_pct = sunday_on_off / weekday_on_off if \
                weekday_on_off > 0 and sunday_on_off > 0 else None
            sat_pct = saturday_per_trip / weekday_per_trip if \
                weekday_per_trip is not None and weekday_per_trip > 0 and saturday_per_trip is not None else None
            sun_pct = sunday_per_trip / weekday_per_trip if \
                weekday_per_trip is not None and weekday_per_trip > 0 and sunday_per_trip is not None else None
            output_data.append(data_structure(
                route_name=route,
                total_weekday_on_off=weekday_on_off,
                total_saturday_on_off=saturday_on_off,
                total_sunday_on_off=sunday_on_off,
                total_weekday_trips=num_weekday_trips,
                total_saturday_trips=num_saturday_trips,
                total_sunday_trips=num_sunday_trips,
                weekday_on_off=weekday_per_trip,
                saturday_on_off=saturday_per_trip,
                sunday_on_off=sunday_per_trip,
                total_sat_pct=total_sat_pct,
                total_sun_pct=total_sun_pct,
                sat_pct=sat_pct,
                sun_pct=sun_pct,
            ))
        data = dataWithColumns(
            data=output_data,
            columns={col: i for col, i in zip(output_columns, range(len(output_columns)))}
        )
        data = self._add_rank('total_weekday_on_off', data.data, data.columns)
        data = self._add_rank('total_saturday_on_off', data.data, data.columns)
        data = self._add_rank('total_sunday_on_off', data.data, data.columns)
        data = self._add_rank('total_weekday_trips', data.data, data.columns)
        data = self._add_rank('total_saturday_trips', data.data, data.columns)
        data = self._add_rank('total_sunday_trips', data.data, data.columns)
        data = self._add_rank('weekday_on_off', data.data, data.columns)
        data = self._add_rank('saturday_on_off', data.data, data.columns)
        data = self._add_rank('sunday_on_off', data.data, data.columns)
        data = self._add_rank('total_sat_pct', data.data, data.columns, description_precision=2)
        data = self._add_rank('total_sun_pct', data.data, data.columns, description_precision=2)
        data = self._add_rank('sat_pct', data.data, data.columns, description_precision=2)
        data = self._add_rank('sun_pct', data.data, data.columns, description_precision=2)
        return data

    def aggregate_route_name_data(self, csv_data, any_day=False):
        new_column_names = ['route_name', 'day_type_name', 'total_trips', 'total_boardings', 'total_alightings',
                            'total_on_off', 'total_svc_minutes', 'total_svc_layover_minutes', 'total_miles',
                            'total_marginal_mins', 'total_marginal_miles', 'alightings_per_trip',
                            'alightings_per_svc_minute', 'alightings_per_svc_layover_minute', 'alightings_per_mile',
                            'alightings_per_marginal_min', 'alightings_per_marginal_mile', 'boardings_per_trip',
                            'boardings_per_svc_minute', 'boardings_per_svc_layover_minute', 'boardings_per_mile',
                            'boardings_per_marginal_min', 'boardings_per_marginal_mile', 'on_off_per_trip',
                            'on_off_per_svc_minute', 'on_off_per_svc_layover_minute', 'on_off_per_mile',
                            'on_off_per_marginal_min', 'on_off_per_marginal_mile']
        new_columns = dict()
        for i in range(len(new_column_names)):
            new_columns[new_column_names[i]] = i

        all_routes = set(line[csv_data.columns['route_name']] for line in csv_data.data)

        route_data = []
        for route in all_routes:
            route_data.extend(self._aggregate_route_name(route, new_column_names, csv_data, any_day))
        data = dataWithColumns(data=route_data, columns=new_columns)
        data = self._add_rank('alightings_per_trip', data.data, data.columns)
        data = self._add_rank('alightings_per_svc_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('alightings_per_svc_layover_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('alightings_per_mile', data.data, data.columns)
        data = self._add_rank('alightings_per_marginal_min', data.data, data.columns, description_precision=2)
        data = self._add_rank('alightings_per_marginal_mile', data.data, data.columns, description_precision=2)
        data = self._add_rank('boardings_per_trip', data.data, data.columns)
        data = self._add_rank('boardings_per_svc_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('boardings_per_svc_layover_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('boardings_per_mile', data.data, data.columns)
        data = self._add_rank('boardings_per_marginal_min', data.data, data.columns, description_precision=2)
        data = self._add_rank('boardings_per_marginal_mile', data.data, data.columns, description_precision=2)
        data = self._add_rank('on_off_per_trip', data.data, data.columns)
        data = self._add_rank('on_off_per_svc_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('on_off_per_svc_layover_minute', data.data, data.columns, description_precision=2)
        data = self._add_rank('on_off_per_mile', data.data, data.columns)
        data = self._add_rank('on_off_per_marginal_min', data.data, data.columns, description_precision=2)
        data = self._add_rank('on_off_per_marginal_mile', data.data, data.columns, description_precision=2)
        return data

    def aggregate_stop_data(self, csv_data):
        csv_data = self._filter_csv_data(csv_data)
        return self._aggregate_stop_data(csv_data, None, None)

    @staticmethod
    def aggregate_stop_trends(csv_data):
        all_route_stops = list()
        for line in csv_data.data:
            to_add = (line[csv_data.columns['route_name']], line[csv_data.columns['stop_id']],
                      line[csv_data.columns['route_variant']][-1:])
            if to_add not in all_route_stops:
                all_route_stops.append(to_add)

        output_columns = ['route_name', 'direction', 'stop_id', 'stop_name', 'total_weekday_on_off',
                          'total_saturday_on_off', 'total_sunday_on_off', 'total_weekday_trips', 'total_saturday_trips',
                          'total_sunday_trips', 'weekday_on_off', 'saturday_on_off', 'sunday_on_off', 'sat_pct',
                          'sun_pct']
        data_structure = namedtuple('MbtaData', output_columns)
        output_data = list()
        for route, stop, direction in all_route_stops:
            route_stop_data = [line for line in csv_data.data if line[csv_data.columns['route_name']] == route and
                               line[csv_data.columns['stop_id']] == stop and
                               line[csv_data.columns['route_variant']][-1:] == direction]
            weekday_on_off = sum(line[csv_data.columns['avg_on_off']] * line[csv_data.columns['total_trips']]
                                 for line in route_stop_data
                                 if line[csv_data.columns['day_type_name']] == 'weekday')
            num_weekday_trips = sum([line[csv_data.columns['total_trips']] for line in route_stop_data
                                    if line[csv_data.columns['day_type_name']] == 'weekday'])
            saturday_on_off = sum(line[csv_data.columns['avg_on_off']] * line[csv_data.columns['total_trips']]
                                  for line in route_stop_data if
                                  line[csv_data.columns['day_type_name']] == 'saturday')
            num_saturday_trips = sum([line[csv_data.columns['total_trips']] for line in route_stop_data if
                                      line[csv_data.columns['day_type_name']] == 'saturday'])
            sunday_on_off = sum(line[csv_data.columns['avg_on_off']] * line[csv_data.columns['total_trips']]
                                for line in route_stop_data if
                                line[csv_data.columns['day_type_name']] == 'sunday')
            num_sunday_trips = sum([line[csv_data.columns['total_trips']] for line in route_stop_data if
                                    line[csv_data.columns['day_type_name']] == 'sunday'])
            weekday_per_trip = weekday_on_off / num_weekday_trips if num_weekday_trips > 0 else None
            saturday_per_trip = saturday_on_off / num_saturday_trips if num_saturday_trips > 0 else None
            sunday_per_trip = sunday_on_off / num_sunday_trips if num_sunday_trips > 0 else None
            sat_pct = saturday_per_trip / weekday_per_trip if \
                weekday_per_trip is not None and weekday_per_trip > 0 and saturday_per_trip is not None else None
            sun_pct = sunday_per_trip / weekday_per_trip if \
                weekday_per_trip is not None and weekday_per_trip > 0 and sunday_per_trip is not None else None
            output_data.append(data_structure(
                route_name=route,
                direction=direction,
                stop_id=stop,
                stop_name=route_stop_data[0][csv_data.columns['stop_name']],
                total_weekday_on_off=weekday_on_off,
                total_saturday_on_off=saturday_on_off,
                total_sunday_on_off=sunday_on_off,
                total_weekday_trips=num_weekday_trips,
                total_saturday_trips=num_saturday_trips,
                total_sunday_trips=num_sunday_trips,
                weekday_on_off=weekday_per_trip,
                saturday_on_off=saturday_per_trip,
                sunday_on_off=sunday_per_trip,
                sat_pct=sat_pct,
                sun_pct=sun_pct,
            ))
        return dataWithColumns(
            data=output_data,
            columns={col: i for col, i in zip(output_columns, range(len(output_columns)))}
        )

    def _aggregate_stop_data(self, csv_data, marginal_mins_pct, marginal_miles):
        all_routes = set(line[csv_data.columns['gtfs_route_id']] for line in csv_data.data)
        route_data = []
        for route in all_routes:
            route_data.extend(self._aggregate_route_by_stop(route, csv_data, marginal_mins_pct, marginal_miles))
        return dataWithColumns(data=route_data, columns=self._stop_data_aggregation_columns())

    def aggregate_subroute_stop_data(self, stops, gtfs_route_ids, all_data, old_columns, subroute_name,
                                     marginal_mins_pct, marginal_miles):
        all_data = self._filter_csv_data(dataWithColumns(all_data, old_columns)).data
        route_data = [line for line in all_data if line[old_columns['gtfs_route_id']] in gtfs_route_ids]

        stops_data = self._aggregate_subroute_stop_data(stops, route_data, old_columns, subroute_name,
                                                        marginal_mins_pct, marginal_miles)
        # excluded_stops_data = self._aggregate_subroute_stop_data(excluded_stops, route_data, old_columns,
        #                                                          f'comp-{subroute_name}', None, None)

        return dataWithColumns(data=stops_data.data, columns=stops_data.columns)

    def _aggregate_subroute_stop_data(self, stops, route_data, old_columns, subroute_name, marginal_mins_pct,
                                      marginal_miles):
        if marginal_mins_pct is None or marginal_miles is None:
            assert(marginal_mins_pct is None)
            assert(marginal_miles is None)
        else:
            assert(marginal_mins_pct > 0)
            assert(marginal_miles > 0)

        new_route_data = []
        route_data_structure = namedtuple('MbtaData', sorted(old_columns.keys(), key=lambda x: old_columns[x]))
        for i in range(len(route_data)):
            is_before = False
            is_after = False
            line = route_data[i]
            if line[old_columns['stop_id']] not in stops:
                if i + 1 < len(route_data):
                    if route_data[i + 1][old_columns['stop_id']] in stops and \
                            route_data[i + 1][old_columns['gtfs_route_id']] == line[old_columns['gtfs_route_id']] and \
                            route_data[i + 1][old_columns['day_type_name']] == line[old_columns['day_type_name']] and \
                            route_data[i + 1][old_columns['gtfs_trip_id']] == line[old_columns['gtfs_trip_id']]:
                        is_before = True
                if i > 0:
                    if route_data[i - 1][old_columns['stop_id']] in stops and \
                            route_data[i - 1][old_columns['gtfs_route_id']] == line[old_columns['gtfs_route_id']] and \
                            route_data[i - 1][old_columns['day_type_name']] == line[old_columns['day_type_name']] and \
                            route_data[i - 1][old_columns['gtfs_trip_id']] == line[old_columns['gtfs_trip_id']]:
                        is_after = True
                if not is_before and not is_after:
                    continue

            data_dict = {col: line[idx] for col, idx in old_columns.items()}.copy()
            data_dict['route_id'] = subroute_name
            data_dict['gtfs_route_id'] = subroute_name
            data_dict['route_variant'] = 'subroute'
            data_dict['stop_sequence'] = 'various'
            if is_after:
                data_dict['load_'] = float(data_dict['load_']) - float(data_dict['boardings']) + \
                                     float(data_dict['alightings'])
                data_dict['stop_id'] = 'after'
                data_dict['stop_name'] = 'after'
            if is_before:
                data_dict['stop_id'] = 'before'
                data_dict['stop_name'] = 'before'
            if is_before or is_after:
                data_dict['boardings'] = 0
                data_dict['alightings'] = 0
            new_route_data.append(route_data_structure(**data_dict))

        return self._aggregate_stop_data(dataWithColumns(data=new_route_data, columns=old_columns), marginal_mins_pct,
                                         marginal_miles / (len(stops) + 1) if marginal_miles else None)

    @staticmethod
    def _add_rank(column_name, data, columns, description_precision=1):
        col_names = sorted(columns.keys(), key=lambda x: columns[x])  # ascending
        old_col_names = col_names.copy()
        new_col_name = f'{column_name}_rank'
        new_day_col_name = f'{column_name}_day_rank'
        new_description_col_name = f'{column_name}_rank_description'
        col_names.append(new_col_name)
        if 'day_type_name' in old_col_names:
            col_names.append(new_day_col_name)
        col_names.append(new_description_col_name)
        new_columns = {col_names[i]: i for i in range(len(col_names))}
        new_data_structure = namedtuple('MbtaData', col_names)
        new_data_with_departure_time = []
        data = sorted(data, key=lambda data_line: data_line[columns[column_name]] if
                      data_line[columns[column_name]] is not None else 0, reverse=True)  # descending

        max_rank = 0
        day_description_dict = {}
        for line in data:
            if line[columns[column_name]] is None:
                continue
            if 'day_type_name' in old_col_names:
                data_dict = {col: line[columns[col]] for col in old_col_names}
                day = data_dict['day_type_name']
                if day not in day_description_dict:
                    day_description_dict[day] = 0
                day_description_dict[day] += 1
            max_rank += 1

        rank = 1
        day_rank_dict = {}
        for line in data:
            data_dict = {col: line[columns[col]] for col in old_col_names}
            if 'day_type_name' in old_col_names:
                day = data_dict['day_type_name']
                if day not in day_rank_dict:
                    day_rank_dict[day] = 0
                day_rank_dict[day] += 1
                data_dict[new_day_col_name] = day_rank_dict[day]
            data_dict[new_col_name] = rank
            line_max_rank = max_rank if 'day_type_name' not in old_col_names else \
                day_description_dict[data_dict['day_type_name']]
            line_rank = rank if 'day_type_name' not in old_col_names else \
                day_rank_dict[data_dict['day_type_name']]
            data_dict[new_description_col_name] = \
                f'{round(data_dict[column_name], description_precision)} (rank {line_rank} of {line_max_rank})' if \
                data_dict[column_name] is not None else None
            new_data_with_departure_time.append(new_data_structure(**data_dict))
            rank += 1
        return dataWithColumns(data=new_data_with_departure_time, columns=new_columns)

    def _aggregate_route(self, route, new_columns, csv_data, any_day):
        route_data = [line for line in csv_data.data if line[csv_data.columns['gtfs_route_id']] == route]
        route_name_data = [line for line in csv_data.data if
                           line[csv_data.columns['route_name']] == route_data[0][csv_data.columns['route_name']]]
        route_ids = set([line[csv_data.columns['route_name']] for line in route_data])
        route_days = set([line[csv_data.columns['day_type_name']] for line in route_data]) if \
            len(route_ids) == 1 else {}
        route_days = route_days if len(route_days) > 0 and not any_day else {'any'}

        output_rows = []
        for day in route_days:
            output_rows.append(self._aggregate_route_on_day(day, route_data, new_columns, csv_data.columns,
                                                            route_name_data))
        return output_rows

    def _aggregate_route_name(self, route, new_columns, csv_data, any_day):
        route_data = [line for line in csv_data.data if line[csv_data.columns['route_name']] == route]
        route_days = set([line[csv_data.columns['day_type_name']] for line in route_data]) if not any_day else {'any'}

        output_rows = []
        for day in route_days:
            output_rows.append(self._aggregate_route_name_on_day(day, route_data, new_columns, csv_data.columns))
        return output_rows

    def _aggregate_route_by_stop(self, route, csv_data, marginal_mins_pct, marginal_miles):
        route_data = [line for line in csv_data.data if line[csv_data.columns['gtfs_route_id']] == route]
        route_ids = set([line[csv_data.columns['route_id']] for line in route_data])
        route_days = set([line[csv_data.columns['day_type_name']] for line in route_data]) if \
            len(route_ids) == 1 else {}

        output_rows = []
        for day in route_days:
            output_rows.extend(self._aggregate_route_by_stop_on_day(day, route_data, csv_data, marginal_mins_pct,
                                                                    marginal_miles))
        return output_rows

    def _aggregate_route_by_stop_on_day(self, day, route_data, csv_data, marginal_mins_pct, marginal_miles):
        route_day_data = [line for line in route_data if line[csv_data.columns['day_type_name']] == day]
        all_trip_ids = set([line[csv_data.columns['gtfs_trip_id']] for line in route_day_data])
        sum_peak_load = 0
        for trip in all_trip_ids:
            sum_peak_load += max([float(line[csv_data.columns['load_']]) for line in route_day_data
                                  if line[csv_data.columns['gtfs_trip_id']] == trip])
        avg_peak_load = sum_peak_load / len(all_trip_ids)
        longest_trip_id = max([line[csv_data.columns['gtfs_trip_id']] for line in route_day_data],
                              key=lambda trip_id: len([line for line in route_day_data if
                                                       line[csv_data.columns['gtfs_trip_id']] == trip_id]))
        longest_trip_data = [line for line in route_day_data if
                             line[csv_data.columns['gtfs_trip_id']] == longest_trip_id]
        stops_on_route = [line[csv_data.columns['stop_id']] for line in longest_trip_data]
        num_trips = len(all_trip_ids)
        output_data_structure = namedtuple('MbtaData', self._stop_data_aggregation_column_names())
        output_rows = []
        for i in range(len(stops_on_route)):
            stop = stops_on_route[i]
            stop_data_points = [line for line in route_day_data if
                                line[csv_data.columns['stop_id']] ==
                                longest_trip_data[i][csv_data.columns['stop_id']]]
            stop_trips = [line[csv_data.columns['gtfs_trip_id']] for line in stop_data_points]
            prior_stop_data_points = [line for line in route_day_data if
                                      line[csv_data.columns['stop_id']] ==
                                      longest_trip_data[i - 1][csv_data.columns['stop_id']] and
                                      line[csv_data.columns['gtfs_trip_id']] in stop_trips] \
                if i > 0 else None
            prior_stop_trips = [line[csv_data.columns['gtfs_trip_id']] for line in prior_stop_data_points] \
                if prior_stop_data_points is not None else None
            stop_data_points_with_prior_trip = [line for line in stop_data_points if
                                                line[csv_data.columns['gtfs_trip_id']] in prior_stop_trips] \
                if prior_stop_data_points is not None else None
            if stop_data_points_with_prior_trip is not None:
                if len(stop_data_points_with_prior_trip) != len(prior_stop_data_points):
                    print("stop data point lengths different", len(stop_data_points_with_prior_trip),
                          len(prior_stop_data_points),
                          route_day_data[0][csv_data.columns['gtfs_route_id']],
                          route_day_data[0][csv_data.columns['route_id']])
            stop_data_point = stop_data_points[0]
            prior_stop_data_point = prior_stop_data_points[0] if prior_stop_data_points else None
            total_boardings = sum([float(line[csv_data.columns['boardings']])
                                   for line in stop_data_points])
            total_alightings = sum([float(line[csv_data.columns['alightings']])
                                    for line in stop_data_points])
            total_pax = sum([float(line[csv_data.columns['load_']]) for line in stop_data_points])
            total_on_off = total_boardings + total_alightings
            if i > 0:
                total_service_seconds = \
                    sum(max(self._time_to_seconds(line1[csv_data.columns['departure_time']]) -
                            self._time_to_seconds(line2[csv_data.columns['departure_time']]), 0) for line1, line2 in
                        zip(stop_data_points_with_prior_trip, prior_stop_data_points))
            else:
                total_service_seconds = 0
            if marginal_mins_pct is not None and marginal_mins_pct != 0 and total_service_seconds > 0:
                total_marginal_svc_seconds = total_service_seconds - (total_service_seconds / (1 + marginal_mins_pct))
            else:
                total_marginal_svc_seconds = total_service_seconds
            if prior_stop_data_point:
                avg_miles = self._distance_miles(stop_data_point[csv_data.columns['lat']],
                                                 prior_stop_data_point[csv_data.columns['lat']],
                                                 stop_data_point[csv_data.columns['long']],
                                                 prior_stop_data_point[csv_data.columns['long']])
            else:
                avg_miles = 0
            avg_marginal_miles = marginal_miles if marginal_miles and marginal_miles != 0 else avg_miles
            output_rows.append(
                output_data_structure(
                    route_name=route_day_data[0][csv_data.columns['route_id']],
                    gtfs_route_id=route_day_data[0][csv_data.columns['gtfs_route_id']],
                    route_variant=route_day_data[0][csv_data.columns['route_variant']],
                    day_type_name=day,
                    stop_name=stop_data_point[csv_data.columns['stop_name']],
                    stop_id=stop,
                    stop_sequence=stop_data_point[csv_data.columns['stop_sequence']],
                    total_boardings=total_boardings,
                    total_alightings=total_alightings,
                    total_pax=total_pax,
                    total_on_off=total_on_off,
                    avg_boardings=total_boardings / num_trips,
                    avg_alightings=total_alightings / num_trips,
                    avg_pax=total_pax / num_trips,
                    route_avg_peak_load=avg_peak_load,
                    avg_on_off=total_on_off / num_trips,
                    total_trips=num_trips,
                    total_svc_seconds=total_service_seconds,
                    avg_svc_seconds=total_service_seconds / len(stop_data_points_with_prior_trip) if
                    stop_data_points_with_prior_trip is not None and len(stop_data_points_with_prior_trip) > 0 else
                    total_service_seconds / num_trips,
                    total_miles=avg_miles * num_trips,
                    avg_miles=avg_miles,
                    total_marginal_svc_seconds=total_marginal_svc_seconds,
                    avg_marginal_svc_seconds=total_marginal_svc_seconds / len(stop_data_points_with_prior_trip) if
                    stop_data_points_with_prior_trip is not None and len(stop_data_points_with_prior_trip) > 0 else
                    total_marginal_svc_seconds / num_trips,
                    total_marginal_miles=avg_marginal_miles * num_trips,
                    avg_marginal_miles=avg_marginal_miles,
                )
            )

        return output_rows

    def _aggregate_route_on_day(self, day, route_data, new_columns, old_columns, route_name_data):
        route_day_data = [line for line in route_data if line[old_columns['day_type_name']] == day] \
            if day != 'any' else route_data
        route_name_data = [line for line in route_name_data if line[old_columns['day_type_name']] == day] \
            if day != 'any' else route_data
        first_stop_data = route_day_data[0]
        total_trips = first_stop_data[old_columns['total_trips']]
        total_route_name_trips = self._determine_num_trips_for_route_on_day(route_name_data, old_columns)
        total_boardings = sum(line[old_columns['total_boardings']] for line in route_day_data)
        total_alightings = sum(line[old_columns['total_alightings']] for line in route_day_data)
        total_on_off = total_boardings + total_alightings
        total_svc_minutes = sum(line[old_columns['total_svc_seconds']] for line in route_day_data) / 60.0
        total_layover_mins = 10 * total_trips
        total_svc_layover_minutes = total_svc_minutes + total_layover_mins if total_svc_minutes != 0 else 0
        total_marginal_mins = sum(line[old_columns['total_marginal_svc_seconds']] for line in route_day_data) / 60.0 + \
            (total_layover_mins if first_stop_data[old_columns['route_variant']] != 'subroute' else 0)
        total_marginal_miles = sum(line[old_columns['total_marginal_miles']] for line in route_day_data)
        total_miles = sum(line[old_columns['total_miles']] for line in route_day_data)
        output_data_structure = namedtuple('MbtaData', new_columns)
        return output_data_structure(
            route_name=first_stop_data[old_columns['route_name']],
            gtfs_route_id=first_stop_data[old_columns['gtfs_route_id']],
            route_variant=first_stop_data[old_columns['route_variant']],
            day_type_name=day,
            total_trips=total_trips,
            total_boardings=total_boardings,
            total_alightings=total_alightings,
            total_on_off=total_on_off,
            total_svc_minutes=total_svc_minutes,
            total_svc_layover_minutes=total_svc_layover_minutes,
            total_miles=total_miles,
            total_marginal_mins=total_marginal_mins,
            total_marginal_miles=total_marginal_miles,
            boardings_per_trip=total_boardings / total_trips,
            boardings_per_svc_minute=total_boardings / total_svc_minutes if total_svc_minutes != 0 else None,
            boardings_per_svc_layover_minute=total_boardings / total_svc_layover_minutes
                if total_svc_layover_minutes != 0 else None,
            boardings_per_mile=total_boardings / total_miles if total_miles != 0 else None,
            boardings_per_marginal_min=total_boardings / total_marginal_mins if total_marginal_mins != 0 else None,
            boardings_per_marginal_mile=total_boardings / total_marginal_miles if total_marginal_miles != 0 else None,
            alightings_per_trip=total_alightings / total_trips,
            alightings_per_svc_minute=total_alightings / total_svc_minutes if total_svc_minutes != 0 else None,
            alightings_per_svc_layover_minute=total_alightings / total_svc_layover_minutes
                if total_svc_layover_minutes != 0 else None,
            alightings_per_mile=total_alightings / total_miles if total_miles != 0 else None,
            alightings_per_marginal_min=total_alightings / total_marginal_mins if total_marginal_mins != 0 else None,
            alightings_per_marginal_mile=total_alightings / total_marginal_miles if total_marginal_miles != 0 else None,
            on_off_per_trip=total_on_off / total_trips,
            on_off_per_svc_minute=total_on_off / total_svc_minutes if total_svc_minutes != 0 else None,
            on_off_per_svc_layover_minute=total_on_off / total_svc_layover_minutes
                if total_svc_layover_minutes != 0 else None,
            on_off_per_mile=total_on_off / total_miles if total_miles != 0 else None,
            on_off_per_marginal_min=total_on_off / total_marginal_mins if total_marginal_mins != 0 else None,
            on_off_per_marginal_mile=total_on_off / total_marginal_miles if total_marginal_miles != 0 else None,
            avg_max_load=first_stop_data[old_columns['route_avg_peak_load']],
            avg_load_busiest_pair=max([float(line[old_columns['avg_pax']]) for line in route_day_data]),
            pct_of_route_service=total_trips / float(total_route_name_trips)
        )

    @staticmethod
    def _aggregate_route_name_on_day(day, route_data, new_columns, old_columns):
        route_day_data = [line for line in route_data if line[old_columns['day_type_name']] == day] \
            if day != 'any' else route_data
        total_trips = sum(line[old_columns['total_trips']] for line in route_day_data)
        total_boardings = sum(line[old_columns['total_boardings']] for line in route_day_data)
        total_alightings = sum(line[old_columns['total_alightings']] for line in route_day_data)
        total_on_off = total_boardings + total_alightings
        total_svc_minutes = sum(max(line[old_columns['total_svc_minutes']], 0) for line in route_day_data)
        total_svc_layover_minutes = sum([line[old_columns['total_svc_layover_minutes']] for line in route_day_data])
        total_miles = sum(line[old_columns['total_miles']] for line in route_day_data)
        total_marginal_mins = sum(line[old_columns['total_marginal_mins']] for line in route_day_data)
        total_marginal_miles = sum(line[old_columns['total_marginal_miles']] for line in route_day_data)
        output_data_structure = namedtuple('MbtaData', new_columns)
        return output_data_structure(
            route_name=route_day_data[0][old_columns['route_name']],
            day_type_name=day,
            total_trips=total_trips,
            total_boardings=total_boardings,
            total_alightings=total_alightings,
            total_on_off=total_on_off,
            total_svc_minutes=total_svc_minutes,
            total_svc_layover_minutes=total_svc_layover_minutes,
            total_miles=total_miles,
            total_marginal_mins=total_marginal_mins,
            total_marginal_miles=total_marginal_miles,
            boardings_per_trip=total_boardings / total_trips,
            boardings_per_svc_minute=total_boardings / total_svc_minutes if total_svc_minutes != 0 else None,
            boardings_per_svc_layover_minute=total_boardings / total_svc_layover_minutes \
                if total_svc_layover_minutes != 0 else None,
            boardings_per_mile=total_boardings / total_miles if total_miles != 0 else None,
            boardings_per_marginal_min=total_boardings / total_marginal_mins if total_marginal_mins != 0 else None,
            boardings_per_marginal_mile=total_boardings / total_marginal_miles if total_marginal_miles != 0 else None,
            alightings_per_trip=total_alightings / total_trips,
            alightings_per_svc_minute=total_alightings / total_svc_minutes if total_svc_minutes != 0 else None,
            alightings_per_svc_layover_minute=total_alightings / total_svc_layover_minutes
                if total_svc_layover_minutes != 0 else None,
            alightings_per_mile=total_alightings / total_miles if total_miles != 0 else None,
            alightings_per_marginal_min=total_alightings / total_marginal_mins if total_marginal_mins != 0 else None,
            alightings_per_marginal_mile=total_alightings / total_marginal_miles if total_marginal_miles != 0 else None,
            on_off_per_trip=total_on_off / total_trips,
            on_off_per_svc_minute=total_on_off / total_svc_minutes if total_svc_minutes != 0 else None,
            on_off_per_svc_layover_minute=total_on_off / total_svc_layover_minutes
                if total_svc_layover_minutes != 0 else None,
            on_off_per_mile=total_on_off / total_miles if total_miles != 0 else None,
            on_off_per_marginal_min=total_on_off / total_marginal_mins if total_marginal_mins != 0 else None,
            on_off_per_marginal_mile=total_on_off / total_marginal_miles if total_marginal_miles != 0 else None,
        )

    @staticmethod
    def _determine_num_trips_for_route_on_day(route_data, columns):
        seen_routes = set()
        num_trips = 0
        for line in route_data:
            if line[columns['gtfs_route_id']] not in seen_routes:
                seen_routes.add(line[columns['gtfs_route_id']])
                num_trips += line[columns['total_trips']]
        return num_trips

    def _distance_miles(self, lat1, lat2, long1, long2):
        origin_lat = self._to_radians_from_degrees(lat1)
        origin_long = self._to_radians_from_degrees(long1)
        dest_lat = self._to_radians_from_degrees(lat2)
        dest_long = self._to_radians_from_degrees(long2)

        delta_lat = (origin_lat - dest_lat) / 2
        delta_long = (origin_long - dest_long) / 2
        delta_lat = math.pow(math.sin(delta_lat), 2)
        delta_long = math.pow(math.sin(delta_long), 2)
        origin_lat = math.cos(origin_lat)
        dest_lat = math.cos(dest_lat)
        haversine = delta_lat + origin_lat * dest_lat * delta_long
        haversine = 2 * 3959 * math.asin(math.sqrt(haversine))
        return haversine

    @staticmethod
    def _filter_csv_data(input_data):
        return dataWithColumns(
            data=[r for r in input_data.data if
                  r[input_data.columns['lat']] is not None and
                  r[input_data.columns['long']] is not None and
                  r[input_data.columns['gtfs_route_id']] is not None and
                  r[input_data.columns['gtfs_trip_id']] is not None and
                  r[input_data.columns['departure_time']] is not None
                  ],
            columns=input_data.columns
        )

    @staticmethod
    def _stop_data_aggregation_column_names():
        return ['route_name', 'gtfs_route_id', 'route_variant', 'day_type_name', 'stop_name', 'stop_id',
                'stop_sequence', 'total_boardings', 'total_alightings', 'total_pax', 'route_avg_peak_load',
                'total_on_off', 'avg_boardings', 'avg_alightings', 'avg_pax', 'avg_on_off', 'total_trips',
                'total_svc_seconds', 'avg_svc_seconds', 'total_miles', 'avg_miles', 'total_marginal_svc_seconds',
                'avg_marginal_svc_seconds', 'total_marginal_miles', 'avg_marginal_miles']

    def _stop_data_aggregation_columns(self):
        new_column_names = self._stop_data_aggregation_column_names()

        new_columns = dict()
        for i in range(len(new_column_names)):
            new_columns[new_column_names[i]] = i

        return new_columns

    @staticmethod
    def _time_to_seconds(time_str):
        hours, minutes, secs = time_str.split(':')
        return int(hours) * 3600 + int(minutes) * 60 + int(secs)

    @staticmethod
    def _to_radians_from_degrees(degrees):
        return degrees * math.pi / 180
