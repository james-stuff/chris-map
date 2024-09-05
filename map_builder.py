"""Run as a command-line tool.  Builds the map.html file
    to be pushed to GitHub for use in GitHub pages"""
import os
import pandas as pd
import arrow
import re
from numpy import nan
import requests
from bs4 import BeautifulSoup as bs
from itertools import product
import gpxpy
from gpxpy import geo
import folium
import geojson
import argparse


def build_map(from_existing_csv: bool = False):
    """run every time new hike(s) are added"""
    if from_existing_csv:
        df_hikes = pd.read_csv("HikeDetails.csv", parse_dates=[0])
    else:
        df_hikes = generate_hike_details_csv(
            cumulatively_find_gpx_files(
                all_known_hikes()
            ).dropna(subset="GPX")
        )

    m = folium.Map(location=(51.5, -0.15), tiles=folium.TileLayer("cartodb positron", name="Clear"), zoom_start=9)
    folium.TileLayer('https://tile.thunderforest.com/transport/{z}/{x}/{y}.png?apikey=a23a350629204ae8b1e22f0729186cb1',
                     attr='&copy; <a href="http://www.thunderforest.com/">Thunderforest</a>, &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
                     name="Railways").add_to(m)
    fg_by_year = {year: folium.FeatureGroup(name=f"{year}")
                  for year in pd.unique(df_hikes["Date"].dt.year)}
    walks_on_map, aggregate_distance = 0, 0
    for ind in df_hikes.index:
        hike_data = df_hikes.loc[ind].to_dict()
        year_fg = fg_by_year[hike_data["Date"].year]
        make_line(hike_data).add_to(year_fg)
        walks_on_map += 1
        aggregate_distance += hike_data["Distance"]

    for yfg in fg_by_year.values():
        yfg.add_to(m)
    m.add_child(folium.LayerControl(position='topright', collapsed=False, autoZIndex=True))

    map_title = f"(Almost) every hike Chris has organised for Free Outdoor Trips from London"
    ave_length = aggregate_distance / walks_on_map
    map_sub_title = (f"{walks_on_map} hikes plotted, average length "
                     f"{distance_description(ave_length)}")
    title_html = (f'<h4 style="position:fixed;z-index:100000;bottom:5px;left:20px;background-color:white;" >'
                  f'{map_title}<br>{map_sub_title}</h4>')
    m.get_root().html.add_child(folium.Element(title_html))

    map_file = "page\\map.html"
    m.save(map_file)


def make_line(hike_data: dict) -> folium.GeoJson:
    """create GeoJson feature for the route to be added to the map"""
    points = [pt[::-1] for pt in points_from_file(hike_data["URL"])]
    date = arrow.get(hike_data["Date"])
    tooltip = (f"{date.format('ddd Do MMM YYYY')}<br/>"
               f"{hike_data['Title']}<br/>"
               f"{route_description(hike_data)}<br/>"
               f"{distance_description(hike_data['Distance'])}")
    gj = geojson.FeatureCollection([geojson.LineString(points)])
    return folium.GeoJson(
        gj,
        style_function=lambda feature: {"color": "blue", "opacity": 0.3, "weight": 5},
        highlight_function=lambda feature: {"color": "red", "opacity": 1.0, "weight": 3},
        tooltip=tooltip
    )


def distance_description(distance_metres: int) -> str:
    dist_mls, dist_kms = (distance_metres / factor
                          for factor in (1_609, 1_000))
    return f"{dist_mls:.1f} miles / {dist_kms:.1f} km"


def route_description(data: dict) -> str:
    start, end = (data[k] for k in ("Start", "End"))
    if start == end:
        return f"Circular walk from {start}"
    return f"{start} to {end}"


def kill_outdated_points_files(df_details: pd.DataFrame):
    """remove any .pts file that pre-dates its corresponding .gpx file"""
    def file_timestamp(filename_or_url: str) -> float:
        if filename_or_url.isnumeric():
            filename_or_url = f"routes\\{filename_or_url}.pts"
        try:
            return os.path.getmtime(filename_or_url)
        except FileNotFoundError:
            return 0

    df = df_details.loc[:, ["URL", "GPX"]]
    df["Kill"] = df["GPX"].apply(file_timestamp) > df["URL"].apply(file_timestamp)
    for u in df.loc[df["Kill"]]["URL"]:
        if f"{u}.pts" in os.listdir("routes"):
            os.remove(f"routes\\{u}.pts")


def generate_hike_details_csv(df_details: pd.DataFrame) -> pd.DataFrame:
    """produce from scratch the DataFrame containing all necessary details
        for all plottable hikes, and saves to a new .csv file"""
    kill_outdated_points_files(df_details)
    df_stations = build_stations_df()
    starts, ends, distances = ([] for _ in range(3))
    hikes_to_be_plotted = 0     # for diagnostic info only
    for i_hike in df_details.index:
        hike_info = df_details.loc[i_hike].to_dict()
        points = [geo.Location(*pt) for pt in
                  points_from_file(hike_info["URL"])]
        if not points:
            print(f"Parsing gpx data from: {hike_info['GPX']}"
                  f"\n\tfor {hike_info['Title']}, {hike_info['Date']}"
                  f"\n\t{hikes_to_be_plotted=}")
            points = gpxpy_points_from_gpx_file(hike_info["GPX"])
            points_to_file(points, hike_info["URL"])
        s, e = (find_proximate_station(points[i_pt], df_stations)
                for i_pt in (0, -1))
        starts.append(s)
        ends.append(e)
        distances.append(get_total_distance(points))
        hikes_to_be_plotted += 1
    df_end_points = pd.DataFrame(
        {
            "URL": df_details["URL"],
            "Start": starts,
            "End": ends,
            "Distance": distances,
        }
    )
    df_details = pd.merge(left=df_details, right=df_end_points, how="left", on="URL")
    df_details = fill_blanks_in_hike_details(df_details)
    df_details.to_csv("HikeDetails.csv", index=False)
    return df_details


def fill_blanks_in_hike_details(df_in: pd.DataFrame) -> pd.DataFrame:
    df_manual = pd.read_excel("ManualStartEnd.ods", engine="odf", usecols=[2, 4, 5], converters={"URL": str})
    df_manual = pd.merge(left=df_in[["Date", "URL"]], right=df_manual, how="left", on="URL")
    return df_in.fillna(df_manual)


def gpxpy_points_from_gpx_file(filepath: str,
                               reduce_points_to: int = 500) -> [geo.Location]:
    """Read in a route as list of points ready to be used
        for calculations for the map"""
    with open(filepath, encoding="utf-8") as gpx_file:
        gpx = gpxpy.parse(gpx_file)
    no_of_points = len(gpx.tracks[0].segments[0].points)
    if no_of_points > 8_000:
        no_of_points = no_of_points // 10
    gpx.reduce_points(max_points_no=no_of_points)
    assert len(gpx.tracks) == 1
    assert len(gpx.tracks[0].segments) == 1
    return gpx.tracks[0].segments[0].points


def points_from_file(url: str) -> [(float,)]:
    """read from specified points file (url, no extension)
        to list of tuple (lat, long), or empty list if
        file doesn't exist"""
    points_file = f"{url}.pts"
    if points_file in os.listdir("routes"):
        with open(f"routes\\{points_file}") as file:
            return [eval(ln) for ln in file.read().split("\n")]
    return []


def points_to_file(points: [geo.Location], filename_stem: str):
    """save a list of gpxpy points to file (no extension).
        Overwrites any existing file with the same name"""
    folder, filename = "routes", f"{filename_stem}.pts"
    # if filename not in os.listdir(folder):
    with open(f"{folder}\\{filename}", "w") as file:
        file.write("\n".join(
            f"({pt.latitude}, {pt.longitude})"
            for pt in points
        ))


def get_total_distance(route: [geo.Location]) -> int:
    """for given route, cumulatively sum the distance between each point
        to get the length of the route in metres"""
    distance = 0
    last_pt = route[0]
    for i, pt in enumerate(route[1:]):
        distance += pt.distance_2d(last_pt)
        last_pt = pt
    return int(distance)


def find_proximate_station(location: geo.Location,
                           df_station_locations: pd.DataFrame, tolerance_metres: int = 500) -> str:
    tolerance_degrees = tolerance_metres * 9e-6
    func_locals = locals()
    query_string = " & ".join(
        f"({co_ord} {'<' if bound == 'max' else '>'} {eval(f'location.{co_ord}', globals(), func_locals)} "
        f"{'+' if bound == 'max' else '-'} {tolerance_degrees})"
        for bound, co_ord in [*product(("max", "min"), ("latitude", "longitude"))]
    )
    stations_subset = df_station_locations.query(query_string)
    if len(stations_subset) > 0:
        return stations_subset.iat[0, 0]
    return nan


def build_stations_df() -> pd.DataFrame:
    def remove_unnecessary_words(full_name: str) -> str:
        return full_name.replace(" Rail Station", "")

    df_stations = pd.read_csv(
        f"uk-train-stations.csv",
        usecols=[1, 2, 3],
        converters={"station_name": remove_unnecessary_words}
    )
    df_tube = pd.read_csv(
        f"Stations 20180921.csv",
        usecols=[2, 8, 9],
        names=["station_name", "longitude", "latitude"],
        skiprows=[0]
    )
    return pd.concat([df_stations, df_tube])


def all_known_hikes() -> pd.DataFrame:
    df_hist = all_historic_hikes()
    df_scraped = hikes_from_subsequent_scrapes()
    return pd.concat([df_hist, df_scraped]).reset_index(drop=True)


def all_historic_hikes() -> pd.DataFrame:
    df_historic_scrape = hikes_from_original_meetup_scrape()
    df_man = pd.read_excel("ManuallyAddedHikes.ods", sheet_name="Sheet1", engine="odf",
                           dtype={"URL": str})
    return pd.concat([df_historic_scrape, df_man]).reset_index(drop=True)


def hikes_from_original_meetup_scrape() -> pd.DataFrame:
    """Load all hikes captured by the selenium scrape of the Past Events page
        on 11th March 2024 (going back to first ever hike on 13th January 2019)"""
    with open("Hikes.txt", "r") as file:
        text = file.read()
    queries = {
        "Date": r"\D{3}, \D{3} \d+, \d{4}",
        "Title": r"\n.+\nThis event has passed",
        "Attendees": r"\d+ attendees,",
        "URL": r"\d{9}"
    }
    data = []
    shift_dates = [arrow.Arrow(2022, 11, 4), arrow.Arrow(2023, 10, 27)]
    for walk in re.finditer(queries["Date"], text):
        date = arrow.get(walk.group()[5:], "MMM D, YYYY").naive
        if arrow.get(date) in shift_dates:
            date = arrow.get(date).shift(days=1).naive
        sub_text = text[walk.start():]
        title, attendees, url = (
            re.search(q, sub_text).group()
            for k, q in queries.items()
            if k != "Date"
        )
        title = title.split("\n")[1]
        attendees = int(attendees[:attendees.index(" ")])
        data.append([date, title, attendees, url])
    df_hikes = pd.DataFrame(data, columns=queries.keys())
    df_hikes["Source"] = "Free"
    return df_hikes


def hikes_from_subsequent_scrapes() -> pd.DataFrame:
    scraped_file = "ScrapedHikes.csv"
    if scraped_file in os.listdir():
        return pd.read_csv(
            scraped_file, converters={"Date": pd.Timestamp, "URL": str})
    return pd.DataFrame({})


def cumulatively_find_gpx_files(df_hikes: pd.DataFrame, sub_folders: [str] = None) -> pd.DataFrame:
    """Given hike data and an ordered list of sub-folders (of the gpx folder),
        will add a GPX column filled in with file paths from the first uploader,
        with each successive uploader's data being used to fill in any remaining gaps"""
    df_hikes["GPX"] = nan
    if not sub_folders:
        sub_folders = filter(lambda sf: sf.isnumeric(), os.listdir("gpx"))
    for i, person in enumerate(sub_folders):
        gpx_series = pd.merge(
            left=df_hikes.drop("GPX", axis=1),
            right=gpx_provided_by(person),
            how="left", on="Date"
        )["GPX"]
        df_hikes["GPX"] = df_hikes["GPX"].fillna(gpx_series)
    return df_hikes


def gpx_provided_by(provider: str) -> pd.DataFrame:
    """Returns a dated list of all gpx files found in each gpx sub-folder"""
    folder_address = f"gpx\\{provider}\\"
    file_data = []
    suunto_date_pattern = r"\d{4}-\d{2}-\d{2}"
    for gpx_file in os.listdir(folder_address):
        if gpx_file[-4:] == ".gpx":
            if "suuntoapp-" in gpx_file:
                found_date = arrow.get(re.search(suunto_date_pattern, gpx_file).group()).date()
            else:
                found_date = get_date_of_gpx_file(f"{folder_address}{gpx_file}")
            file_data.append(
                [
                    pd.Timestamp(found_date),
                    f"{folder_address}{gpx_file}"
                ]
            )
    return pd.DataFrame(file_data, columns=["Date", "GPX"])


def get_date_of_gpx_file(file_path: str) -> arrow.Arrow.date:
    with open(f"{file_path}", encoding="utf-8") as gf:
        gpx_text = gf.read()
        found_time = re.search("<time>.+</time>", gpx_text)
        if found_time:
            return arrow.get(found_time.group()[6:16]).date()


def check_and_update_meetup_events():
    df_new = scrape_past_events_for_chris_hikes().sort_values(by="Date")
    scraped_file = "ScrapedHikes.csv"
    if scraped_file in os.listdir():
        df_existing = pd.read_csv(scraped_file, converters={"URL": str})
        existing_urls = df_existing["URL"].to_list()
        df_to_add = df_new.drop(df_new.loc[df_new["URL"].isin(existing_urls)].index)
        if len(df_to_add):
            pd.concat([df_existing, df_to_add]).to_csv(scraped_file, index=False, date_format="%Y-%m-%d")
    else:
        df_new.to_csv(scraped_file, index=False)


def scrape_past_events_for_chris_hikes() -> pd.DataFrame:
    url = "https://www.meetup.com/free-outdoor-trips-from-london/events/?type=past"
    response = requests.get(url)
    html = response.text
    event_details = []
    soup = bs(html, "html5lib")
    past_events = soup.find("div", "flex min-h-[28px] flex-col space-y-4 xs:w-full md:w-3/4")
    for event in past_events.find_all(id=re.compile("^ep-")):
        image = event.find("img")
        if image["alt"] == "Photo of Christopher":
            raw_date, title = [*event.strings][:2]
            date_string = arrow.get(
                re.search(r"\D{3} \d+, \d{4}", raw_date).group(),
                "MMM D, YYYY").format("YYYY-MM-DD")
            attendees = [*event.strings][3]
            attendees = int(attendees[:attendees.index(" ")])
            url = re.search(r"\d{9}", event.find("a")["href"]).group()
            event_details.append((date_string, title, attendees, url, "Free"))
    return pd.DataFrame(data=event_details, columns=["Date", "Title", "Attendees", "URL", "Source"])


def ensure_correct_date_in_gpx_file(folder_path: str, file_fragment: str, correct_ymd: (int,)):
    """produce a new version of a gpx file with correct date"""
    filename = [*filter(lambda fn: re.search(file_fragment, fn) and
                        fn[-4:] == ".gpx",
                        os.listdir(folder_path))][0]
    correct_date = arrow.Arrow(*correct_ymd)
    correct_time = correct_date.format('YYYY-MM-DDTHH:mm:ssZ')
    print(f"Setting date to {correct_time} for {folder_path}\\{filename}")
    with open(f"{folder_path}\\{filename}", encoding="utf-8") as file:
        text = file.read()
    incorrect_date = get_date_of_gpx_file(f"{folder_path}\\{filename}")
    if incorrect_date:
        new_text = text.replace(f"{incorrect_date}", correct_date.format("YYYY-MM-DD"))
    else:
        metadata = f"\n<metadata>\n\t<time>{correct_time}</time>\n</metadata>"
        found_metadata = re.search("<metadata>.+</metadata>", text)
        if found_metadata:
            new_text = text.replace(
                "<metadata>",
                f"<metadata>\n\t<time>{correct_time}</time>\n"
            )
        else:
            insert_at_position = re.search("<gpx .+>", text).end()
            new_text = text[:insert_at_position] + metadata + text[insert_at_position:]
    with open(
            f"{folder_path}\\{filename[:-4]}_time-corrected.gpx", "w",
            encoding="utf-8"
    ) as new_file:
        new_file.write(new_text)
    os.rename(
        f"{folder_path}\\{filename}",
        f"{folder_path}\\{filename[:-4]}._gpx",
    )


def snip_at(file_path: str, station_name: str, discard_before: bool = True):
    """Snip off unwanted part of a .gpx file, at either a named station
        or (lat, long) tuple.  Discard the section either before
        or after the snip"""
    if isinstance(station_name, str):
        snip_location = locate_station(station_name)
    else:
        print(f"Supplied co-ordinates: {station_name}")
        snip_location = geo.Location(*station_name)
    gpx = gpxpy.parse(open(file_path, "r", encoding="utf-8"))
    os.rename(file_path, file_path.replace(".", "._"))
    all_points = gpx.tracks[0].segments[0].points
    if not discard_before:
        all_points = all_points[::-1]
    for i, point in enumerate(all_points):
        if point.distance_2d(snip_location) < 100:
            if not discard_before:
                i = len(all_points) - 1 - i
            print(f"Point {i} of {len(gpx.tracks[0].segments[0].points)} is close enough to {station_name} station")
            discard_segment, new_segment = gpx.tracks[0].segments[0].split(i)
            if not discard_before:
                new_segment, discard_segment = discard_segment, new_segment
            gpx.tracks[0].segments[0] = new_segment
            xml = gpx.to_xml()
            with open(f"{file_path[:-4]}-snipped{file_path[-4:]}", "w", encoding="utf-8") as output_file:
                output_file.write(xml)
            break
    print(f"There are now {len(gpx.tracks[0].segments[0].points)} points.")
    return snip_location


def locate_station(station_name: str) -> geo.Location:
    df_stations = build_stations_df()
    station_pos = df_stations.query(
        f"station_name.str.startswith('{station_name}')"
    ).iloc[0, [1, 2]].to_list()
    return geo.Location(*station_pos)


def plot_one_hike(url: str):
    """Utility function to verify a route looks good before committing to it"""
    print("\nPlotting a single hike:")
    df_one = all_known_hikes().query(f"URL == '{url}'").reset_index(drop=True)
    df_hike_dets = generate_hike_details_csv(
        cumulatively_find_gpx_files(df_one)
    )
    print(df_hike_dets)
    build_map(True)


def missing_hikes(start_year: int = 2019) -> pd.DataFrame:
    """Utility function to generate a table of hikes for which data is still needed"""
    def event_page_url_stem(src: str) -> str:
        group = "free-outdoor-trips-from-london" if src == "Free" else "metropolitan-walkers"
        return f"https://www.meetup.com/{group}/events/"
    df = cumulatively_find_gpx_files(
        all_known_hikes()).sort_values(by="Date", ascending=False)
    df = df.loc[df["GPX"].isnull() & (df["Date"].dt.year >= start_year)][["Date", "Title", "Source", "URL"]]
    df["EventPage"] = df["Source"].apply(event_page_url_stem) + df["URL"]
    df = df.drop(["Source", "URL"], axis=1)
    df.to_csv("gaps.csv")
    return df


def integrated_process():
    """Run as a single process that corrects date for latest
        GPX file and builds map with that route assigned
        to the latest hike without a route"""
    # TODO: current assumed use-case is run on day after a hike
    #       with only one new .gpx file added
    check_and_update_meetup_events()
    hike_date = get_date_of_latest_hike_without_route()
    gpx_file = get_latest_gpx_file()
    sub_folder, filename = gpx_file.split("\\")
    ensure_correct_date_in_gpx_file(
        f"gpx\\{sub_folder}",
        filename,
        (hike_date.year, hike_date.month, hike_date.day)
    )
    build_map()


def get_date_of_latest_hike_without_route() -> pd.Timestamp:
    df_events = hikes_from_subsequent_scrapes()
    df_existing_hikes = pd.read_csv("HikeDetails.csv", parse_dates=[0])
    df_temp = pd.merge(left=df_events, right=df_existing_hikes[["Date", "GPX"]], on="Date", how="left")
    return df_temp.loc[df_temp["GPX"].isna(), "Date"].max()


def get_latest_gpx_file() -> str:
    """Find the most recent file added to the gpx tree.
        Returns <gpx sub-folder name>\\<filename>"""
    latest_gpx = ""
    for sub_folder in os.listdir("gpx"):
        gpx_sf = f"gpx\\{sub_folder}"
        sf_latest = max(
            os.listdir(gpx_sf),
            key=lambda f: os.path.getmtime(f"{gpx_sf}\\{f}")
        )
        if ((not latest_gpx) or
                os.path.getmtime(f"{gpx_sf}\\{sf_latest}") >
                os.path.getmtime(f"gpx\\{latest_gpx}")):
            latest_gpx = f"{sub_folder}\\{sf_latest}"
    return latest_gpx


def df_from_gpx(path: str) -> pd.DataFrame:
    """Make a DataFrame containing all gpx points in the file"""
    with open(path, encoding="utf-8") as gpx_file:
        gpx = gpxpy.parse(gpx_file)
    points = gpx.tracks[0].segments[0].points
    data = {
        prop: [eval(f"pt.{prop}") for pt in points]
        for prop in ("latitude", "longitude", "elevation", "time")
    }
    return pd.DataFrame(data)


def df_with_diffs_from_gpx(path: str) -> pd.DataFrame:
    df = df_from_gpx(path)
    df["time_diff"] = df["time"].diff()
    dist_data = df[["latitude", "longitude", "elevation"]].values
    df["dist"] = [0 if i == 0 else geo.distance(*pt, *dist_data[i - 1]) for i, pt in enumerate(dist_data)]
    df["pace"] = df["dist"] / df["time_diff"].dt.seconds
    # ns_diff = df["latitude"].diff().apply(lambda d: d * 10_000_000 / 90)
    # ew_diff = df["longitude"].diff()
    # bearing = []
    # for i, dd in enumerate(df["dist"]):
    #     b = 0
    #     if dd != 0 and i != 0:
    #         angle = math.degrees(math.acos(ns_diff[i] / dd))
    #         if ew_diff[i] > 0:
    #             b = angle
    #         else:
    #             b = 360 - angle
    #     bearing.append(b)
    # df["bearing"] = bearing
    return df


if __name__ == "__main__":
    my_parser = argparse.ArgumentParser(description='Map builder')
    my_parser.add_argument('Operation',
                           metavar='operation',
                           type=str,
                           help='either [B] build map or [S] scrape meetup for new events')
    args = my_parser.parse_args()
    op = args.Operation.upper()

    options = {
        "A": integrated_process,
        "B": build_map,
        "S": check_and_update_meetup_events,
    }
    if op in options:
        options[op]()
    else:
        print(f"{op} is not a valid operation code")
