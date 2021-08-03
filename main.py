if __name__ == "__main__":
    from mbta_bus_ridership.data_reader import DataReader
    from mbta_bus_ridership.data_merger import DataMerger
    from mbta_bus_ridership.data_analyzer import DataAnalyzer
    from mbta_bus_ridership.data_writer import DataWriter
    from mbta_bus_ridership.csv_reader import dataWithColumns


    def to_analyze_subroute(ridership_data, aggregated_data):
        def analyze_subroute(subroute_stops, subroute_gtfs_routes, marginal_min_pct, marginal_miles, subroute_name):
            subroute_data = DataAnalyzer().aggregate_subroute_stop_data(
                subroute_stops, subroute_gtfs_routes, ridership_data.data, ridership_data.columns, subroute_name,
                marginal_min_pct, marginal_miles)

            DataWriter().write_csv(subroute_data.data, subroute_data.columns,
                                   f'./output/subroutes/{subroute_name}_stop_output.csv')

            all_data_with_subroute = dataWithColumns(data=aggregated_data.data + subroute_data.data,
                                                     columns=aggregated_data.columns)

            aggregated_subroute_route_data = DataAnalyzer().aggregate_route_data(all_data_with_subroute)
            DataWriter.write_csv(aggregated_subroute_route_data.data, aggregated_subroute_route_data.columns,
                                 f'./output/subroutes/{subroute_name}_route_output.csv')

            aggregated_subroute_frequent_route_data = DataAnalyzer().aggregate_frequent_route_data(
                all_data_with_subroute)
            DataWriter.write_csv(aggregated_subroute_frequent_route_data.data,
                                 aggregated_subroute_frequent_route_data.columns,
                                 f'./output/subroutes/{subroute_name}_frequent_route_output.csv')

            # aggregated_subroute_route_data_any_day = \
            #     DataAnalyzer().aggregate_route_data(all_data_with_subroute, any_day=True)
            # DataWriter.write_csv(aggregated_subroute_route_data_any_day.data,
            #                      aggregated_subroute_route_data_any_day.columns,
            #                      f'./subroutes/{subroute_name}_route_output_any_day.csv')

            aggregated_subroute_name_data = DataAnalyzer().aggregate_route_name_data(aggregated_subroute_route_data)
            DataWriter().write_csv(aggregated_subroute_name_data.data, aggregated_subroute_name_data.columns,
                                   f'./output/subroutes/{subroute_name}_route_name_output.csv')

            # aggregated_subroute_name_data_any_day = \
            #     DataAnalyzer().aggregate_route_name_data(aggregated_subroute_route_data, any_day=True)
            # DataWriter().write_csv(aggregated_subroute_name_data_any_day.data,
            #                        aggregated_subroute_name_data_any_day.columns,
            #                        f'./subroutes/{subroute_name}_route_name_output_any_day.csv')
        return analyze_subroute

    def run():
        ridership_data_with_columns = DataReader().read_ridership()

        gtfs_data = DataReader().read_gtfs_data()
        ridership_data_with_columns = DataMerger().merge(ridership_data_with_columns, gtfs_data)

        del gtfs_data

        aggregated_data = DataAnalyzer().aggregate_stop_data(ridership_data_with_columns)
        DataWriter().write_csv(aggregated_data.data, aggregated_data.columns,
                               f'./output/stop_output_{len(aggregated_data.data)}.csv')

        aggregated_stop_trends = DataAnalyzer().aggregate_stop_trends(aggregated_data)
        DataWriter().write_csv(aggregated_stop_trends.data, aggregated_stop_trends.columns,
                               f'./output/stop_ons_offs_{len(aggregated_stop_trends.data)}.csv')

        aggregated_route_day_data = DataAnalyzer().aggregate_route_day_data(aggregated_stop_trends)
        DataWriter().write_csv(aggregated_route_day_data.data, aggregated_route_day_data.columns,
                               f'./output/route_ons_offs_{len(aggregated_route_day_data.data)}.csv')

        aggregated_route_data = DataAnalyzer().aggregate_route_data(aggregated_data)
        DataWriter().write_csv(aggregated_route_data.data, aggregated_route_data.columns,
                               f'./output/route_output_{len(aggregated_route_data.data)}.csv')

        aggregated_frequent_route_data = DataAnalyzer().aggregate_frequent_route_data(aggregated_data)
        DataWriter().write_csv(aggregated_frequent_route_data.data, aggregated_frequent_route_data.columns,
                               f'./output/frequent_route_output_{len(aggregated_frequent_route_data.data)}.csv')

        # aggregated_route_data_without_days = DataAnalyzer().aggregate_route_data(aggregated_data, any_day=True)
        # DataWriter().write_csv(aggregated_route_data_without_days.data, aggregated_route_data_without_days.columns,
        #                        f'route_output_{len(aggregated_route_data_without_days.data)}_any_day.csv')

        aggregated_route_name_data = DataAnalyzer().aggregate_route_name_data(aggregated_route_data)
        DataWriter().write_csv(aggregated_route_name_data.data, aggregated_route_name_data.columns,
                               f'./output/route_name_output_{len(aggregated_route_name_data.data)}.csv')

        # aggregated_route_name_data_any_day = \
        #     DataAnalyzer().aggregate_route_name_data(aggregated_route_data, any_day=True)
        # DataWriter().write_csv(aggregated_route_name_data_any_day.data, aggregated_route_name_data_any_day.columns,
        #                        f'route_name_output_{len(aggregated_route_name_data_any_day.data)}_any_day.csv')

        analyze_subroute = to_analyze_subroute(ridership_data_with_columns, aggregated_data)

        analyze_subroute(['104', '106', '107', '108', '109', '110'], [311], None, None, '1-Central-Harvard')
        analyze_subroute(['110', '2168', '2166', '2167', '66', '67', '68', '69', '71'], [312], None, None,
                         '1-Harvard-Central')
        analyze_subroute(['64', '1', '2', '6', '10003'], [311], None, None, '1-Nubian-MassAve')
        analyze_subroute(['10100', '10101', '62', '63', '64'], [312], None, None, '1-MassAve-Nubian')
        analyze_subroute(['6', '10003', '57', '58'], [311], 3.0, 0.4, '1-EastOfWashington-ToHarvard')
        analyze_subroute(['854', '856', '10100', '10101'], [312], 2.75, 0.4, '1-EastOfWashington-ToNubian')

        analyze_subroute(['225', '226', '230', '231'], [162], 0.25, 0.4, '4-NEnd-inbound')
        analyze_subroute(['233', '234', '30235'], [165], 0.01, 0.2, '4-NEnd-outbound')
        analyze_subroute(['221', '21599', '242', '247', '30249'], [164], None, None, '4-SeaportBlvd-outbound')
        analyze_subroute(['31257', '31256', '214', '11599', '2116'], [163], None, None, '4-SeaportBlvd-inbound')
        analyze_subroute(['6564', '888', '889', '31255', '31257'], [165], None, None, '4-SummerSt-outbound')
        analyze_subroute(['30249', '30256', '210', '212', '890', '891', '892'], [162], None, None, '4-SummerSt-inbound')
        analyze_subroute(['12891', '11891', '190', '191', '117', '113'], [163], None, None, '4-downtown-to-NSta')
        analyze_subroute(['114', '30203', '65471', '16551'], [164], None, None, '4-NSta-to-downtown')

        analyze_subroute(['33', '10033', '34'], [282, 284], None, None, '7-wrong-direction-inbound')
        analyze_subroute(['32', '10031', '10032', '33'], [288], None, None, '7-wrong-direction-outbound')

        analyze_subroute(['111', '31111', '41111', '51111'], [216, 231], None, None, '8-UMass-inbound')
        analyze_subroute(['138', '139', '140', '142'], [230, 255], None, None, '8-UMass-outbound')
        analyze_subroute(['10010', '11241', '11242', '11244'], [230], 4.0, 0.8, '8-SBayMall-outbound')
        analyze_subroute(['29049', '29051', '29052', '9955'], [216], 3.0, 0.8, '8-SBayMall-inbound')
        analyze_subroute(['128', '9960', '10'], [216], 3.0, 0.6, '8-MassAve-inbound')
        analyze_subroute(['8', '20009', '30009'], [230], 1.0, 0.4, '8-MassAve-outbound')
        analyze_subroute(['128', '9960', '10'], [216, 231], 3.0, 0.6, '8-MassAve-inbound-all-trips')
        analyze_subroute(['8', '20009', '30009'], [230, 255], 1.0, 0.4, '8-MassAve-outbound-all-trips')
        analyze_subroute(['1787', '1788', '1789', '21158', '11158', '5089'], [230, 255], 2.0, 0.9,
                         '8-MedicalCenterDetour-outbound')
        analyze_subroute(['10014', '10005', '5090', '10015', '1790'], [216, 231], 1.25, 0.4,
                         '8-MedicalCenterDetour-inbound')

        analyze_subroute(['86178', '86179', '86180'], [55, 57, 100, 101], 4.5, 4.2, '76-Hanscom')
        analyze_subroute(['8437', '8438'], [100, 101], 0.75, 0.4, '627-BedfordToWorthen')
        analyze_subroute(['7911', '7912'], [100, 101], 0.4, 0.4, '627-WorthenToBedford')

        analyze_subroute(['2353', '12432', '2354', '2355', '2356', '2357', '2358', '2359', '2360', '12360',
                          '12323', '32358', '22358', '7922'], [61], None, None, '78-toArlingtonHeights')
        analyze_subroute(['7922', '17922', '6201', '6202', '6203', '6204', '6217', '2324', '6218', '2326',
                          '2327', '2328', '2329', '2330', '2331'], [62], None, None, '78-fromArlingtonHeights')
        analyze_subroute(['2464', '2465', '2466', '2467', '2468', '12468', '2469', '2470', '2471', '2472',
                          '2473', '2361', '2323', '2324', '6218', '2326', '2327', '2328', '2329', '2330', '2331'],
                         [63], None, None, '78-fromArlmont')
        analyze_subroute(['2353', '12432', '2354', '2355', '2356', '2357', '2358', '2359', '2360', '2361',
                          '2323', '2483', '2484', '2485', '2486', '2487', '2488', '2489', '2490', '2491',
                          '12465', '2493', '2494', '2495', '2464'], [54], None, None, '78-toArlmont')
        analyze_subroute(['2361', '2323'], [54, 63], 1.5, 0.4, '78-ParkCircle')
        analyze_subroute(['2464', '2465', '2466', '2467', '2468', '12468', '2469', '2470', '2471', '2472',
                          '2473', '2324', '6218', '2326', '2327', '2328', '2329', '2330', '2331'],
                         [25], None, None, '84-fromArlmont')
        analyze_subroute(['2353', '12432', '2354', '2355', '2356', '2357', '2358', '2201', '2202', '2203', '2464'],
                         [26], None, None, '84-toArlmont')
        analyze_subroute(['2353', '12432', '2354', '2355', '2356', '2357', '2358', '2359', '2360', '12360',
                          '12323', '32358', '22358', '7922'], [20, 22, 27, 61, 100], None, None,
                         '62-78-toArlingtonHeights')
        analyze_subroute(['7922', '17922', '6201', '6202', '6203', '6204', '6217', '2324', '6218', '2326',
                          '2327', '2328', '2329', '2330', '2331'], [21, 23, 24, 62, 101], None, None,
                         '62-78-fromArlingtonHeights')


    # Excitingly, the weekday ridership of ~400k bus trips aligns with mbtabackontrack!
    # GTFS data read works, but the shapes.txt file is not parsed and would be most useful for determining distance
    #  between stops

    # Want to add latitude, longitude, route_id to CSV data

    run()
