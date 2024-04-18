import map_builder as mb
import arrow
from numpy import dtype


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
    for n in range(1, 4):
        df_gpx = mb.gpx_provided_by(f"0{n}")
        assert len(df_gpx) > 10
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
    assert data_length == 12
    assert len(full_df[(full_df["Start"] == "") | (full_df["End"] == "")]) == 0
