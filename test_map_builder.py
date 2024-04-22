import os

import map_builder as mb
import arrow
import pandas as pd
from numpy import dtype
from gpxpy import geo


def test_all_hikes():
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
    for n in range(1, 5):
        df_gpx = mb.gpx_provided_by(f"0{n}")
        assert len(df_gpx) > 4
        assert len(df_gpx.columns) == 2
        assert df_gpx.dtypes.to_list() == [dtype('<M8[ns]'), dtype('O')]
    df_all_gpx = mb.cumulatively_find_gpx_files(mb.all_historic_hikes())
    assert len(df_all_gpx.dropna(subset="GPX")) > 116
    print(df_all_gpx.dropna(subset="GPX"))
    # TODO: test for conversion of suunto files
    # TODO: two different processes: use existing HikeDetails.csv and/or generate from scratch?
    #       probably the former.  This could be pushed to github
    #       Probably don't store .gpx files on github.  If any have changed, they should
    #       be renamed by the system and this would appear as a change in HikeDetails.csv


def test_scraping_meetup():
    mb.check_and_update_meetup_events()


def test_build_hikes_table():
    print(mb.all_known_hikes())
    print(mb.all_known_hikes().info())
    df_all_hikes = mb.cumulatively_find_gpx_files(mb.all_known_hikes()).dropna(subset="GPX")
    df = mb.generate_hike_details_for_map(df_all_hikes)
    print(df)
    expected_columns = {"Date", "Title", "Attendees", "URL", "Source", "Start", "End", "Distance", "GPX"}
    assert set(df.columns) == expected_columns
    data_rows = len(df)
    assert data_rows >= 118
    assert all(isinstance(d, int) for d in df["Distance"])
    assert len(df.dropna()) == data_rows


def test_build_from_existing():
    mb.build_map(from_existing_csv=True)


def test_gap_filling():
    df_iman_info = mb.cumulatively_find_gpx_files(mb.all_known_hikes(), ["03"]).dropna(subset="GPX")
    print(df_iman_info)
    df = mb.generate_hike_details_for_map(df_iman_info)
    full_df = mb.fill_blanks_in_hike_details(df)
    print(full_df)
    data_length = len(full_df)
    assert data_length == 11
    assert len(full_df[(full_df["Start"] == "") | (full_df["End"] == "")]) == 0


def test_correcting_timestamps():
    # Todo: make this a test rather than an operation
    corrections = {
        "VGW": arrow.Arrow(2020, 7, 26),
    }
    for route, date in corrections.items():
        mb.correct_time_for_manually_generated_gpx(route, date)


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
    """ 1. drop new .gpx file in appropriate folder
        2. new file is detected (because it is not in HikeDetails.csv?  Because it succeeds map.html?)
        3. new points file is created
        4. new or updated HikeDetails.csv is created
        Whole process should take less than ten seconds"""
    # todo: next step is delete any points file whose age is greater
    #       than that of its corresponding .gpx file?
    os.rename("routes\\300371735.pts", "routes\\300371735._pts")
    start_time = arrow.now().timestamp()
    mb.build_map()
    changed_files = ["HikeDetails.csv", "page\\map.html"]
    for cf in changed_files:
        assert os.path.getmtime(cf) > start_time
        print(f"{cf=} size={os.path.getsize(cf):,}")
    assert os.path.getsize(changed_files[0]) > 13_000
    assert os.path.getsize(changed_files[1]) > 1_440_000
    assert arrow.now().timestamp() - start_time < 10
    df = pd.read_csv(changed_files[0])
    print(df)
    assert (len(df.loc[df["Start"].isna()]) +
            len(df.loc[df["End"].isna()]) == 0)


def plot_one_hike(url: str):
    df_one = mb.all_historic_hikes().query(f"URL == '{url}'").reset_index(drop=True)
    df_hike_dets = mb.generate_hike_details_for_map(
        mb.cumulatively_find_gpx_files(df_one)
    )
    print(df_hike_dets)
    mb.build_map(True)

def test_plot_one_hike_only():
    plot_one_hike("272128450")
