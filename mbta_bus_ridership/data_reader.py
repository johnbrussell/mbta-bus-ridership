from mbta_bus_ridership.csv_reader import CsvReader
import json
import os
import gtfs_parsing.analyses.analyses as gtfs_parser
from gtfs_parsing.data_structures.data_structures import gtfsSchedules, uniqueRouteInfo, stopDeparture


class DataReader:
    def read_gtfs_data(self):
        analyses = gtfs_parser.determine_analysis_parameters(self._load_gtfs_configuration())
        analysis = analyses[0]
        data = self._remove_trips_that_do_not_operate_within_analysis_timeframe(
            self._read_gtfs_data(analysis, os.path.join("data", "gtfs"))
        )
        data = self._remove_irrelevant_gtfs_data(data)
        return self._flatten_gtfs_departure_times(data)

    @staticmethod
    def read_ridership():
        data = CsvReader().read(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                             'data', 'bus_trip_ridership.csv'))
        return data

    @staticmethod
    def _flatten_gtfs_departure_times(gtfs_data):
        for trip, trip_info in gtfs_data.tripSchedules.items():
            for stop in trip_info.tripStops.keys():
                departure_time = trip_info.tripStops[stop].departureTime
                hour, minute, _ = departure_time.split(':')
                num_simultaneous_departures = len([v for v in trip_info.tripStops.values()
                                                   if v.departureTime.startswith(f'{hour}:{minute}')])
                num_previous_departures = len([k for k, v in trip_info.tripStops.items() if
                                               int(k) < int(stop) and v.departureTime.startswith(f'{hour}:{minute}')])
                hour, minute, _ = departure_time.split(':')
                seconds = round(0 + 60 * num_previous_departures / num_simultaneous_departures)
                assert(0 <= seconds < 60)
                if seconds < 10:
                    seconds = f'0{seconds}'
                else:
                    seconds = str(seconds)
                trip_info.tripStops[stop] = stopDeparture(
                    stopId=trip_info.tripStops[stop].stopId,
                    departureTime=f'{hour}:{minute}:{seconds}'
                )
        return gtfs_data

    @staticmethod
    def _load_gtfs_configuration():
        with open("gtfs_configuration.json") as config_file:
            config = json.load(config_file)
        return config

    @staticmethod
    def _read_gtfs_data(config, data_folder_name):
        data_location = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), data_folder_name)

        return gtfs_parser.parse(config, data_location)

    @staticmethod
    def _remove_irrelevant_gtfs_data(data):
        return gtfsSchedules(
            tripSchedules=data.tripSchedules,
            dateTrips=None,
            uniqueRouteTrips=data.uniqueRouteTrips,
            stopLocations=data.stopLocations,
        )

    @staticmethod
    def _remove_trips_that_do_not_operate_within_analysis_timeframe(raw_data):
        # Function borrowed directly from gtfs-traversal due to bug in gtfs-parser
        all_trips = set()
        all_stops = set()
        for day, trips in raw_data.dateTrips.items():
            all_trips = all_trips.union(trips)
        for trip in all_trips:
            all_stops = all_stops.union(set(s.stopId for s in raw_data.tripSchedules[trip].tripStops.values()))

        new_data = gtfsSchedules(
            tripSchedules={trip_id: trip_info for trip_id, trip_info in raw_data.tripSchedules.items() if
                           trip_id in all_trips},
            dateTrips=raw_data.dateTrips,
            uniqueRouteTrips={route_id: uniqueRouteInfo(tripIds=[t for t in route_info.tripIds if t in all_trips],
                                                        routeInfo=route_info.routeInfo)
                              for route_id, route_info in raw_data.uniqueRouteTrips.items()
                              if any(t in all_trips for t in route_info.tripIds)},
            stopLocations={stop_id: location for stop_id, location in raw_data.stopLocations.items()
                           if stop_id in all_stops},
        )
        return new_data
