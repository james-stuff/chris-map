import map_builder as mb
import os
import arrow
import pandas as pd
from numpy import dtype
from gpxpy import geo
import shutil


def test_historic_hikes():
    df_scraped = mb.hikes_from_original_meetup_scrape()
    assert len(df_scraped) == 192
    assert len(df_scraped.loc[df_scraped["Date"].dt.date == arrow.Arrow(2023, 10, 28).date()]) == 1
    assert arrow.Arrow(2023, 10, 27).date() not in df_scraped["Date"].dt.date.to_list()
    assert len(df_scraped.loc[df_scraped["Date"].dt.date == arrow.Arrow(2022, 11, 5).date()]) == 1
    assert arrow.Arrow(2022, 11, 4).date() not in df_scraped["Date"].dt.date.to_list()
    assert df_scraped.iat[0, 1].startswith("Nature near London - The only moat in Middlesex")
    df_all_historic = mb.all_historic_hikes()
    assert len(df_all_historic) == 215


def test_finding_gpx_files():
    for n in range(1, 8):
        df_gpx = mb.gpx_provided_by(f"0{n}")
        assert len(df_gpx) > 1
        assert len(df_gpx.columns) == 2
        assert df_gpx.dtypes.to_list() == [dtype('<M8[ns]'), dtype('O')]
    df_all_gpx = mb.cumulatively_find_gpx_files(mb.all_historic_hikes())
    assert len(df_all_gpx.dropna(subset="GPX")) > 116
    # TODO: test for conversion of suunto files


def test_scraping_meetup():
    mb.check_and_update_meetup_events()


def test_build_hikes_table():
    df_all_hikes = mb.cumulatively_find_gpx_files(mb.all_known_hikes()).dropna(subset="GPX")
    df = mb.generate_hike_details_csv(df_all_hikes)
    assert os.path.getmtime("HikeDetails.csv") > arrow.now().timestamp() - 10
    expected_columns = {
        "Date": "datetime64[ns]", "Title": "object", "Attendees": "float64",
        "URL": "object", "Source": "object", "GPX": "object",
        "Start": "object", "End": "object", "Distance": "int64"
    }
    assert list(df.columns) == [*expected_columns.keys()]
    data_rows = len(df)
    assert data_rows >= 118
    assert all(df[c].dtype == v for c, v in expected_columns.items())
    assert len(df.dropna()) == data_rows


def test_build_from_existing():
    mb.build_map(from_existing_csv=True)


def test_gap_filling():
    df_iman_info = mb.cumulatively_find_gpx_files(mb.all_known_hikes(), ["03"]).dropna(subset="GPX")
    print(df_iman_info)
    df = mb.generate_hike_details_csv(df_iman_info)
    full_df = mb.fill_blanks_in_hike_details(df)
    print(full_df)
    data_length = len(full_df)
    assert data_length == 11
    assert len(full_df[(full_df["Start"] == "") | (full_df["End"] == "")]) == 0


def test_correcting_timestamps():
    corrections = {
        "Gravesend_Sole_Street_Borough_Green_.gpx": (2021, 8, 14),  # has no date
        "Holland_Park_to_Trafalgar_Square.gpx": (2023, 12, 17),     # has incorrect date
    }
    test_folder = "gpx\\test"
    for filename, ymd in corrections.items():
        mb.ensure_correct_date_in_gpx_file(test_folder, filename, ymd)
        new_fn = f"{filename[:-4]}_time-corrected.gpx"
        assert new_fn in os.listdir(test_folder)
        assert mb.get_date_of_gpx_file(f"{test_folder}\\{new_fn}") == arrow.Arrow(*ymd).date()
        gpx_points = mb.gpxpy_points_from_gpx_file(f"{test_folder}\\{new_fn}")
        assert 300 < len(gpx_points) <= 500
    for corrected_file in [ff for ff in os.listdir(test_folder) if ff[-19:] == "_time-corrected.gpx"]:
        os.remove(f"{test_folder}\\{corrected_file}")
        os.rename(f"{test_folder}\\{corrected_file[:-19]}._gpx", f"{test_folder}\\{corrected_file[:-19]}.gpx")


def verify_valid_points_format(points: [(float,)]) -> bool:
    assert 300 < len(points) < 500
    assert all(isinstance(c, float)
               for pt in points
               for c in pt)
    mid_point = points[len(points) // 2]
    lat, long = mid_point
    assert 45 < lat < 60  # right part of the world?
    assert -10 < long < 10
    return True


def test_working_with_points_files():
    points = mb.points_from_file("299649947")
    verify_valid_points_format(points)
    no_points = mb.points_from_file("not_a_file")
    assert len(no_points) == 0
    assert isinstance(no_points, list)
    test_file = "test_working_with_points_files"
    mb.points_to_file([geo.Location(*pt) for pt in points], test_file)
    test_points = mb.points_from_file(test_file)
    verify_valid_points_format(test_points)
    tf1 = f"{test_file}_1"
    gpx_pts = mb.gpxpy_points_from_gpx_file("gpx\\01\\10641248499.gpx")
    mb.points_to_file(gpx_pts, tf1)
    tp1 = mb.points_from_file(tf1)
    verify_valid_points_format(tp1)
    for del_file in (test_file, tf1):
        os.remove(f"routes\\{del_file}.pts")


def test_add_new_hike_workflow():
    # replace an existing .gpx file with a more up-to-date one in a different folder
    original_dymchurch = "gpx\\02\\7104076057new.gpx"
    test_file = "gpx\\test\\7104076057-Dymchurch-unsnipped.gpx"
    os.rename(original_dymchurch, original_dymchurch.replace(".gpx", "._gpx"))
    shutil.copy(test_file, "gpx\\04\\")
    tf_in_new_home = test_file.replace("test", "04")
    assert os.path.getmtime(tf_in_new_home) > arrow.now().timestamp() - 1
    # add a .gpx file for a completely new hike (simulated by removing its .pts file)
    # in this case, also use Iman's 9th March Nature Near London short .gpx file
    mar_9th_file = "gpx\\03\\09-03-2024._gpx"
    os.remove("routes\\299480822.pts")
    os.rename(mar_9th_file, mar_9th_file.replace("._gpx", ".gpx"))
    df = mb.cumulatively_find_gpx_files(
        mb.all_known_hikes()
    ).dropna(subset="GPX")
    def count_points_files() -> int: return len([f for f in os.listdir("routes") if f[-4:] == ".pts"])
    points_files_count = count_points_files()
    mb.kill_outdated_points_files(df)
    assert count_points_files() == points_files_count - 1
    mb.build_map()
    assert count_points_files() == points_files_count + 1
    os.rename(original_dymchurch.replace(".gpx", "._gpx"), original_dymchurch)
    os.remove("gpx\\04\\7104076057-Dymchurch-unsnipped.gpx")
    os.rename(mar_9th_file.replace("._gpx", ".gpx"), mar_9th_file)


def test_plot_one_hike_only():
    mb.plot_one_hike("272249610")


def test_show_gaps():
    print(mb.missing_hikes(2020))


def test_debug_build():
    mb.build_map()


def test_integrated_process():
    """Build a process that handles the whole thing:
            - scrape for new events
            - find latest .gpx file
            - match it to latest walk without a route
            - change the date on it
            - run the build process"""
    # d = mb.get_date_of_latest_hike_without_route()
    # print("\n", d)
    # assert d.year == 2024
    # assert d.month == 6
    # assert d.day == 29
    assert mb.get_latest_gpx_file() == "08\\track_20240629_103615.gpx"#'01\\8262502218-Portugal - Copy.gpx'
    assert mb.get_date_of_latest_hike_without_route() == pd.Timestamp(2024, 6, 29)
    # mb.integrated_process()

