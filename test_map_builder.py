import map_builder as mb
import os
import arrow
from numpy import dtype
from gpxpy import geo
import shutil
import gpxpy
import re
import polars as pl
from polars.testing import assert_frame_equal


def test_historic_hikes():
    df_scraped = mb.hikes_from_original_meetup_scrape()
    assert len(df_scraped) == 192
    assert len(df_scraped.filter(Date="2023-10-28")) == 1
    assert "2023-10-27" not in df_scraped["Date"]
    assert len(df_scraped.filter(Date="2022-11-05")) == 1
    assert "2022-11-04" not in df_scraped["Date"]
    assert df_scraped.item(0, 1).startswith(
        "Nature near London - The only moat in Middlesex")
    df_all_historic = mb.all_historic_hikes()
    assert len(df_all_historic) == 216


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


def test_correcting_dates():
    corrections = {
        "Gravesend_Sole_Street_Borough_Green_.gpx": "2021-08-14",  # has no date
        "Holland_Park_to_Trafalgar_Square.gpx": "2023-12-17",     # has incorrect date
    }
    test_folder = "gpx\\test"
    for filename, ymd in corrections.items():
        mb.ensure_correct_date_in_gpx_file(test_folder, filename, ymd)
        new_fn = f"{filename[:-4]}_time-corrected.gpx"
        assert new_fn in os.listdir(test_folder)
        assert mb.get_date_of_gpx_file(f"{test_folder}\\{new_fn}") == ymd
        gpx_points = mb.gpxpy_points_from_gpx_file(f"{test_folder}\\{new_fn}")
        assert len(gpx_points) > 800
    for corrected_file in [ff for ff in os.listdir(test_folder) if ff[-19:] == "_time-corrected.gpx"]:
        os.remove(f"{test_folder}\\{corrected_file}")
        os.rename(f"{test_folder}\\{corrected_file[:-19]}._gpx", f"{test_folder}\\{corrected_file[:-19]}.gpx")


def verify_valid_points_format(points: [(float,)]) -> bool:
    assert len(points) > 800
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
        safe_remove(f"routes\\{del_file}.pts")


def safe_remove(path: str):
    if os.path.exists(path):
        os.remove(path)


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
    safe_remove("routes\\299480822.pts")
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
    safe_remove("gpx\\04\\7104076057-Dymchurch-unsnipped.gpx")
    safe_remove("routes\\285486454.pts")
    safe_remove("routes\\299480822.pts")
    os.rename(mar_9th_file.replace("._gpx", ".gpx"), mar_9th_file)


def test_plot_one_hike_only():
    mb.plot_one_hike("302940231")


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
    # assert mb.get_latest_gpx_file() == "08\\track_20240629_103615.gpx"#'01\\8262502218-Portugal - Copy.gpx'
    # assert mb.get_date_of_latest_hike_without_route() == pd.Timestamp(2024, 6, 29)
    # mb.integrated_process()


def test_split_file_at_gaps():
    file_path = f"gpx\\07\\Woking_to_West_Byfleet.gpx"#{mb.get_latest_gpx_file()}"
    with open(file_path, encoding="utf-8") as gpx_file:
        gpx = gpxpy.parse(gpx_file)
    points = gpx.tracks[0].segments[0].points
    for i, p in enumerate(points):
        if i < len(points) - 1:
            dist = geo.distance(
                latitude_1=p.latitude,
                longitude_1=p.longitude,
                elevation_1=None,
                latitude_2=points[i + 1].latitude,
                longitude_2=points[i + 1].longitude,
                elevation_2=None
            )
            if dist > 100:
                print(f"There is a 100-metre-plus gap after {p}")
                print(f"The next point is {points[i + 1]}")
                query = f"<trkpt lat=\"{points[i + 1].latitude}0\" lon=\"{points[i + 1].longitude}0\""
                print(f"{query=}")
                with open(file_path, encoding="utf-8") as gpx_file:
                    file_text = gpx_file.read()
                match = re.search(query, file_text)
                # if match:
                #     assert match.start() == 1092970


def test_station_finding():
    # timeit results, 22nd Oct 2024:
    # Original: 100 loops, best of 5: 2.97 msec per loop
    # improved: 50 loops, best of 5: 4.38 msec per loop
    # 12/02/2026 polars version:
    #   200 loops, best of 5: 1.25 msec per loop
    assert mb.find_proximate_station(
        mb.geo.Location(51.485628, -0.606757)
    ) == "Windsor & Eton Riverside"
    # make sure it's not too slow when there are lots of stations nearby:
    central_london = mb.geo.Location(51.515276, -0.109188)
    print(mb.find_proximate_station(central_london))
    # warn when not close to a station
    chesterford = geo.Location(52.066359, 0.208629)
    mb.find_proximate_station(chesterford)
    df_new = mb.rebuild_hike_details()
    dfh = mb.read_hike_details().join(
        df_new.select("Date", "End"), on="Date", how="left"
    ).filter(pl.col("End") != pl.col("End_right"))
    print(dfh)


def test_migrate_web_scraping_to_json():
    """TDD for adjusting to meetup format change"""
    scraped_file = "ScrapedHikes.csv"
    with open(scraped_file, encoding="utf-8") as sf:
        text = sf.read()
    assert len([*filter(
        lambda h: h.count(",") >= 4, text.split("\n"))]) >= 32
    assert re.search("2024-10-26", text)
    assert re.search("maple canter", text)


def verify_hike_details():
    latest_correct_filename = "1770650777.csv"
    df_correct = pl.read_csv(
        f"Previous Hike Details\\{latest_correct_filename}",
        schema_overrides={"URL": pl.String}
    )
    df_candidate = pl.read_csv(
        f"HikeDetails.csv",
        schema_overrides={"URL": pl.String}
    )
    assert_frame_equal(df_candidate, df_correct)


def test_bs():
    print("Hi", end="")
    print(f"{'\b' * 2}o")


def test_new_load():
    print(mb.new_map())
    # verify_hike_details()


def test_new_process():
    fn_correct, fn_previous = "1770650777.csv", "1769940683.csv"
    roland_file = "Knockholt_via_Hogtrough_Hill_to_Chelsfield"
    os.rename(f"gpx\\11\\{roland_file}._gpx",
              f"{mb.downloads_path}\\{roland_file}.gpx")
    mb.rollback(fn_previous)
    mb.new_build()
    print(f"{mb.read_hike_details()['GPX'].item(-1)=}")
    print(f"{mb.read_hike_details(fn_correct)['GPX'].item(-1)=}")
    assert_frame_equal(
        *(mb.read_hike_details(file)
          for file in ("HikeDetails.csv", fn_correct))
    )


def test_own_file():
    """(no date correction needed)"""
    fn_correct, fn_previous = "1765671608.csv", "1765125041.csv"
    my_file = "Christmas_Party_Hike_2025"
    # os.rename(f"gpx\\07\\{my_file}.gpx",
    #           f"{mb.downloads_path}\\{my_file}.gpx")
    # mb.rollback(fn_previous)
    mb.build_map()
    print(f"{mb.read_hike_details()['GPX'].item(-1)=}")
    print(f"{mb.read_hike_details(fn_correct)['GPX'].item(-1)=}")
    assert_frame_equal(
        *(mb.read_hike_details(file)
          for file in ("HikeDetails.csv", fn_correct))
    )


def test_rebuild():
    df_new = mb.rebuild_hike_details()
    print(f"{df_new=}")
    print(f"{df_new.filter(pl.col('Date').str.starts_with('2025-07-0'))}")
    assert_frame_equal(
        df_new,
        mb.read_hike_details("1770650777.csv").cast({pl.Float64: pl.Int64})
    )
