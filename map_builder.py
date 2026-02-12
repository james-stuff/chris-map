"""Run as a command-line tool.  Builds the map.html file
    to be pushed to GitHub for use in GitHub pages"""
import os
import shutil
import pandas as pd
import polars as pl
import arrow
import re
import requests
from bs4 import BeautifulSoup as bs
import gpxpy
from gpxpy import geo
import folium
import geojson
import argparse
import json
import gpx_folders_key


downloads_path = "C:\\Users\\j_a_c\\Downloads"


def new_map():
    print("Building map:")
    dfh = read_hike_details()
    m = folium.Map(location=(51.5, -0.15), tiles=folium.TileLayer("cartodb positron", name="Clear"), zoom_start=9)
    folium.TileLayer('https://tile.thunderforest.com/transport/{z}/{x}/{y}.png?apikey=a23a350629204ae8b1e22f0729186cb1',
                     attr='&copy; <a href="http://www.thunderforest.com/">Thunderforest</a>, &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
                     name="Railways").add_to(m)
    fg_by_year = {year: folium.FeatureGroup(name=f"{year}")
                  for year in dfh["Date"].str.slice(0, 4).unique()}
    walks_on_map, aggregate_distance = 0, 0
    print("\tHikes on map: ", end=" " * 3)
    for hike in dfh.iter_rows(named=True):
        year_fg = fg_by_year[hike["Date"][:4]]
        make_line(hike).add_to(year_fg)
        walks_on_map += 1
        print(f"{'\b' * 3}{walks_on_map:>3}", end="", flush=True)
        aggregate_distance += hike["Distance"]
    print("")

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


def build_map():
    """assume existing HikeDetails.csv is correct and only add
        new hikes, or re-generate .pts files that are outdated"""
    new_gpx = [
        *filter(
            lambda fn: fn.endswith(".gpx"),
            os.listdir(downloads_path)
        )
    ]
    dfh = read_hike_details()
    mod_times = {
        f"{fld}_mod_time":
            pl.Series([file_timestamp(fou) for fou in dfh[fld]])
        for fld in ("GPX", "URL")
    }
    df_to_update = dfh.with_columns(**mod_times).filter(
        pl.col("GPX_mod_time") > pl.col("URL_mod_time")
    )
    latest_mapped_date = dfh["Date"].max()
    print(f"\n{latest_mapped_date=}")
    df_new = dfh.filter(pl.col("Date").is_null())
    new_hikes = all_known_hikes().filter(
                pl.col("Date") > latest_mapped_date
            )
    if df_to_update.is_empty() and (len(new_hikes) == len(new_gpx) == 1):
        print(f"One .gpx file found: {new_gpx[0]}")
        sf_destination = choose_uploader()
        new_file_name = f"gpx\\{sf_destination}\\{new_gpx[0]}"
        hike_date, hike_title, _, url, _ = new_hikes.row(0)
        print(f"Parsing gpx data from: {new_gpx[0]}"
              f"\n\tfor {hike_title}, {hike_date}")
        points = gpxpy_points_from_gpx_file(f"{downloads_path}\\{new_gpx[0]}")
        points_to_file(points, url)
        if new_gpx[0] in os.listdir(f"gpx\\{sf_destination}"):
            new_file_name += f"{int(arrow.now().timestamp())}"
        os.rename(f"{downloads_path}\\{new_gpx[0]}", new_file_name)
        new_file_name = ensure_correct_date_in_gpx_file(
            f"gpx\\{sf_destination}",
            new_file_name.split("\\")[2],
            hike_date
        )
        df_new = pl.DataFrame(
            [[*new_hikes.row(0), new_file_name, *calculate_hike_particulars(points)]],
            schema=dfh.schema, orient="row"
        )
        pl.concat([dfh, df_new]).write_csv("HikeDetails.csv")
    new_map()
    return df_to_update


def hike_matching_table() -> pl.DataFrame:
    """Table of all known hikes matched with best known .gpx files
        (= first six columns of HikeDetails.csv)
        Takes 1.5sec
        """
    df = all_known_hikes()
    max_sf = max(map(int, filter(
        lambda folder: folder.isnumeric(), os.listdir("gpx"))
                     )
                 )

    def sub_folder(fldr: int) -> str: return f"gpx\\{fldr:02}"

    gpx_data = [
        (
            get_date_of_gpx_file(f"{sub_folder(i_sf)}\\{gpx_file}"),
            f"{sub_folder(i_sf)}\\{gpx_file}"
        )
        for i_sf in range(1, max_sf + 1)
        for gpx_file in filter(lambda fn: fn.endswith(".gpx"),
                               os.listdir(sub_folder(i_sf)))
    ]
    return df.join(
        pl.DataFrame(
            gpx_data, schema=["Date", "GPX"], orient="row"
        ).group_by(
            "Date"
        ).agg(
            pl.col("GPX").first()
        ),
        how="left", on="Date"
    )


"""
    hike matching table: derive from HikeDetails.csv
    if it doesn't exist, create it
    It is Hike Details minus Start, End and Distance
    It could also come from all_known_hikes matched with
        the available GPX files 
        (compare to all_known_hikes as an alternative way of finding new hikes?)
    New hikes, combined with newly-matched hikes and those for which
        the .pts file is out of date, form a new matching table to which
        Start, End and Distance details are added, then appended to the
        existing Hike Details (minus the ones whose .pts were recalculated)
    Only parse .gpx files for the above subset
    Starts, ends, distances can be calculated from the parse
    Then draw all the lines and build the map
"""


def choose_uploader() -> str:
    subfolders = gpx_folders_key.gpx_folders
    sf_key = input(
        f"Who produced this file?\n"
        f"{show_options_list(subfolders.values())}\n"
    ).zfill(2)
    if sf_key in subfolders:
        return sf_key
    return "07"


def show_options_list(numbered_choices: [str]) -> str:
    all_options = [
                      f"[{i + 1}] {c}" for i, c in enumerate(numbered_choices)
                  ] + [f"[N] None of the above"]
    display_string = ""
    max_line_length = 72
    current_line_length = 0
    for option_text in all_options:
        if current_line_length + len(option_text) > max_line_length:
            display_string += "\n"
            current_line_length = 0
        display_string += f"\t{option_text}"
        current_line_length += len(option_text) + 1
    return display_string


def calculate_hike_particulars(route: [geo.Location]) -> tuple[str, str, int]:
    start, end = (
        find_proximate_station(route[i_pt])
        for i_pt in (0, -1)
    )
    distance = get_total_distance(route)
    return start, end, distance


def make_line(hike_data: dict) -> folium.GeoJson:
    """create GeoJson feature for the route to be added to the map"""
    points = points_from_file(hike_data["URL"], longitude_first=True)
    date = arrow.get(hike_data["Date"])
    tooltip = (f"{date.format('ddd Do MMM YYYY')}<br/>"
               f"{hike_data['Title']}<br/>"
               f"{route_description(hike_data)}<br/>"
               f"{distance_description(hike_data['Distance'])}")
    gj = geojson.FeatureCollection([geojson.LineString(points)])
    return folium.GeoJson(
        gj,
        style_function=lambda feature: {"color": "blue", "opacity": 0.3, "weight": 8},
        highlight_function=lambda feature: {"color": "red", "opacity": 1.0, "weight": 3},
        tooltip=tooltip
    )


def distance_description(distance_metres: int | float) -> str:
    dist_mls, dist_kms = (distance_metres / factor
                          for factor in (1_609, 1_000))
    return f"{dist_mls:.1f} miles / {dist_kms:.1f} km"


def route_description(data: dict) -> str:
    start, end = (data[k] for k in ("Start", "End"))
    if start == end:
        return f"Circular walk from {start}"
    return f"{start} to {end}"


def file_timestamp(filename_or_url: str) -> float:
    if not re.search(r"\\", filename_or_url):
        filename_or_url = f"routes\\{filename_or_url}.pts"
    try:
        return os.path.getmtime(filename_or_url)
    except FileNotFoundError:
        return 0


def kill_outdated_points_files(df_details: pd.DataFrame):
    """remove any .pts file that pre-dates its corresponding .gpx file"""
    df = df_details.loc[:, ["URL", "GPX"]]
    df["Kill"] = df["GPX"].apply(file_timestamp) > df["URL"].apply(file_timestamp)
    for u in df.loc[df["Kill"]]["URL"]:
        if f"{u}.pts" in os.listdir("routes"):
            os.remove(f"routes\\{u}.pts")


def rebuild_hike_details() -> pl.DataFrame:
    """from scratch"""
    dfh = hike_matching_table().drop_nulls("GPX")
    dfp = pl.DataFrame(
        [calculate_hike_particulars(
            [geo.Location(*pt) for pt in points_from_file(u)])
            for u in dfh["URL"]],
        schema=["Start", "End", "Distance"], orient="row"
    )
    dfh = pl.concat([dfh, dfp], how="horizontal")
    return fill_blanks_in_hike_details(dfh)


def read_hike_details(filename: str = "HikeDetails.csv") -> pl.DataFrame:
    if filename != "HikeDetails.csv":
        filename = f"Previous Hike Details\\{filename}"
    return pl.read_csv(filename, schema_overrides={"URL": pl.String})


def fill_blanks_in_hike_details(df_in: pl.DataFrame) -> pl.DataFrame:
    df_manual = pl.read_ods(
        "ManualStartEnd.ods", schema_overrides={"URL": pl.String}
    ).select("URL", "Start", "End")
    df_out = df_in.join(df_manual, how="left", on="URL")
    return df_out.with_columns(
        Start=pl.col("Start").fill_null(pl.col("Start_right")),
        End=pl.col("End").fill_null(pl.col("End_right")),
    ).select(pl.exclude("Start_right", "End_right"))


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


def points_from_file(url: str, longitude_first: bool = False) -> [(float,)]:
    """read from specified points file (url, no extension)
        to list of tuple (lat, long), or empty list if
        file doesn't exist"""
    points_file = f"{url}.pts"
    if points_file in os.listdir("routes"):
        df_pts = pl.read_csv(f"routes\\{url}.pts")
        if longitude_first:
            df_pts = df_pts.select("long", "lat")
        return [*df_pts.iter_rows()]
    return []


def points_to_file(points: [geo.Location], filename_stem: str):
    """save a list of gpxpy points to file (no extension).
        Overwrites any existing file with the same name"""
    folder, filename = "routes", f"{filename_stem}.pts"
    pl.DataFrame(
        {
            "lat": [pt.latitude for pt in points],
            "long": [pt.longitude for pt in points]
        }
    ).write_csv(f"{folder}\\{filename}")


def get_total_distance(route: [geo.Location]) -> int:
    """for given route, cumulatively sum the distance between each point
        to get the length of the route in metres"""
    distance = 0
    last_pt = route[0]
    for i, pt in enumerate(route[1:]):
        distance += pt.distance_2d(last_pt)
        last_pt = pt
    return int(distance)


def find_proximate_station(location: geo.Location) -> str | None:
    tolerance_degrees = 0.05    # equates to <= 5km either side

    def ib_args(lat_or_long: str) -> ():
        return (
                location.__getattribute__(f"{lat_or_long}itude") +
                (n * tolerance_degrees)
                for n in (-1, 1)
        )
    stn_subset = df_stations.filter(
        pl.col("latitude").is_between(*ib_args("lat")),
        pl.col("longitude").is_between(*ib_args("long"))
    ).with_columns(
        distance=((pl.col("latitude") - location.latitude) ** 2 +
                  (pl.col("longitude") - location.longitude) ** 2) ** 0.5
    )
    if not stn_subset.is_empty():
        return stn_subset.sort(by="distance")["station_name"].item(0)


def build_stations_df() -> pl.DataFrame:
    df_mainline = pl.read_csv(
        "uk-train-stations.csv", columns=[1, 2, 3]
    ).with_columns(
        pl.col("station_name").str.replace(" Rail Station", "")
    )
    df_tube = pl.read_csv(
        "Stations 20180921.csv", columns=[2, 8, 9]
    ).rename(
        {old: new
         for old, new in zip(["NAME", "y", "x"], df_mainline.columns)}
    ).select(df_mainline.columns)
    return pl.concat([df_mainline, df_tube])


def all_known_hikes() -> pl.DataFrame:
    df_hist = all_historic_hikes()
    df_scraped = hikes_from_subsequent_scrapes()
    return pl.concat([df_hist, df_scraped])


def all_historic_hikes() -> pl.DataFrame:
    df_historic_scrape = hikes_from_original_meetup_scrape()
    df_man = pl.read_ods(
        "ManuallyAddedHikes.ods",
        schema_overrides={"Date": pl.String, "URL": pl.String}
    )
    return pl.concat([df_historic_scrape, df_man])


def hikes_from_original_meetup_scrape() -> pl.DataFrame:
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
        date = arrow.get(walk.group()[5:], "MMM D, YYYY")
        if arrow.get(date) in shift_dates:
            date = arrow.get(date).shift(days=1)
        date = date.format("YYYY-MM-DD")
        sub_text = text[walk.start():]
        title, attendees, url = (
            re.search(q, sub_text).group()
            for k, q in queries.items()
            if k != "Date"
        )
        title = title.split("\n")[1]
        attendees = int(attendees[:attendees.index(" ")])
        data.append([date, title, attendees, url])
    return pl.DataFrame(
        data, schema=[*queries.keys()], orient="row"
    ).with_columns(
        Source=pl.lit("Free")
    )


def hikes_from_subsequent_scrapes() -> pl.DataFrame:
    scraped_file = "ScrapedHikes.csv"
    if scraped_file in os.listdir():
        return pl.read_csv(
            scraped_file,
            schema_overrides={"Date": pl.String, "URL": pl.String}
        )
    return pl.DataFrame({})


def get_date_of_gpx_file(file_path: str) -> str:
    with open(f"{file_path}", encoding="utf-8") as gf:
        gpx_text = gf.read()
        found_time = re.search("<time>.+</time>", gpx_text)
        if found_time:
            return found_time.group()[6:16]


def check_and_update_meetup_events():
    df_new = scrape_past_events_for_chris_hikes().sort(by="Date")
    scraped_file = "ScrapedHikes.csv"
    if scraped_file in os.listdir():
        df_existing = pl.read_csv(scraped_file, schema_overrides={"URL": str})
        existing_urls = df_existing["URL"].to_list()
        df_to_add = df_new.filter(~pl.col("URL").is_in(existing_urls))
        if not df_to_add.is_empty():
            pl.concat([df_existing, df_to_add]).write_csv(scraped_file)
    else:
        df_new.write_csv(scraped_file)


def scrape_past_events_for_chris_hikes() -> pl.DataFrame:
    url = "https://www.meetup.com/free-outdoor-trips-from-london/events/?type=past"
    response = requests.get(url)
    html = response.text
    event_details = []
    soup = bs(html, "lxml")
    json_tag = soup.find("script", {"type": "application/json"})
    js = json.loads(json_tag.text)
    apollo = js['props']['pageProps']['__APOLLO_STATE__']
    event_keys = [
        *filter(lambda k: k.startswith("Event"),
                apollo.keys()
                )
    ]
    for ek in event_keys:
        ev = apollo[ek]
        if ev['eventHosts'][0]['memberId'] == "14080424":
            event_details.append(
                (
                    ev['dateTime'][:10],
                    ev['title'],
                    ev['going']['totalCount'],
                    ev['id'],
                    "Free",
                )
            )
    return pl.DataFrame(
        event_details,
        schema=["Date", "Title", "Attendees", "URL", "Source"],
        orient="row"
    )


def ensure_correct_date_in_gpx_file(
        folder_path: str, file_fragment: str, correct_date: str
) -> str:
    """produce a new version of a gpx file with correct date"""
    filename = [*filter(lambda fn: re.search(file_fragment, fn) and
                        fn[-4:] == ".gpx",
                        os.listdir(folder_path))][0]
    message = f"Setting date to {correct_date} for {folder_path}\\{filename}"
    with open(f"{folder_path}\\{filename}", encoding="utf-8") as file:
        text = file.read()
    existing_date = get_date_of_gpx_file(f"{folder_path}\\{filename}")
    if existing_date:
        if existing_date != correct_date:
            print(message)
            new_text = text.replace(f"{existing_date}", f"{correct_date}")
        else:
            return f"{folder_path}\\{filename}"
    else:
        print(message, "(Date was not present)")
        metadata = f"\n<metadata>\n\t<time>{correct_date}</time>\n</metadata>"
        found_metadata = re.search("<metadata>.+</metadata>", text)
        if found_metadata:
            new_text = text.replace(
                "<metadata>",
                f"<metadata>\n\t<time>{correct_date}</time>\n"
            )
        else:
            insert_at_position = re.search("<gpx .+>", text).end()
            new_text = text[:insert_at_position] + metadata + text[insert_at_position:]
    corrected_file = f"{folder_path}\\{filename[:-4]}_time-corrected.gpx"
    with open(
            corrected_file, "w",
            encoding="utf-8"
    ) as new_file:
        new_file.write(new_text)
    os.rename(
        f"{folder_path}\\{filename}",
        f"{folder_path}\\{filename[:-4]}._gpx",
    )
    return corrected_file


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
    df_stations = build_stations_df().to_pandas()
    station_pos = df_stations.query(
        f"station_name.str.startswith('{station_name}')"
    ).iloc[0, [1, 2]].to_list()
    return geo.Location(*station_pos)


# def plot_one_hike(url: str):
#     """Utility function to verify a route looks good before committing to it"""
#     print("\nPlotting a single hike:")
#     df_one = all_known_hikes().query(f"URL == '{url}'").reset_index(drop=True)
#     df_hike_dets = generate_hike_details_csv(
#         cumulatively_find_gpx_files(df_one)
#     )
#     print(df_hike_dets)
#     build_map(True)


# def missing_hikes(start_year: int = 2019) -> pd.DataFrame:
#     """Utility function to generate a table of hikes for which data is still needed"""
#     def event_page_url_stem(src: str) -> str:
#         group = "free-outdoor-trips-from-london" if src == "Free" else "metropolitan-walkers"
#         return f"https://www.meetup.com/{group}/events/"
#     df = cumulatively_find_gpx_files(
#         all_known_hikes()).sort_values(by="Date", ascending=False)
#     df = df.loc[df["GPX"].isnull() & (df["Date"].dt.year >= start_year)][["Date", "Title", "Source", "URL"]]
#     df["EventPage"] = df["Source"].apply(event_page_url_stem) + df["URL"]
#     df = df.drop(["Source", "URL"], axis=1)
#     df.to_csv("gaps.csv")
#     return df


def integrated_process(sub_folder: int = 7):
    """Run as a single process that corrects date for the latest
        GPX file and builds map with that route assigned
        to the latest hike without a route.  Automatically
        move .gpx file dated today from Downloads to gpx\\07 folder"""
    # TODO: make the process more resilient by tackling:
    #       - filename conflicts
    #       - process is generally confusing
    #       - what other issues have occurred?
    #   Support multi-part hikes/events
    dl = "C:\\Users\\j_a_c\\Downloads"
    downloaded_gpx = [
        *filter(lambda fn:
                fn[-4:] == ".gpx",
                os.listdir(dl))
    ]
    if downloaded_gpx:
        file = sorted(
            downloaded_gpx,
            key=lambda fn: os.path.getmtime(f"{dl}\\{fn}"),
            reverse=True
        )[0]
        os.rename(f"{dl}\\{file}", f"gpx\\{sub_folder:02d}\\{file}")
        # TODO: need generate_hike_details_csv() to have the option of only
        #       returning a DataFrame without overwriting HikeDetails.csv
        #       (see plot_one_hike())
        check_and_update_meetup_events()
        hike_date = get_date_of_latest_hike_without_route()
        gpx_file = get_latest_gpx_file()
        if input(f"Use file {gpx_file} for hike on "
                 f"{hike_date.strftime('%a. %d %B %Y')}? ") not in "Yy":
            return
        sub_folder, filename = gpx_file.split("\\")
        ensure_correct_date_in_gpx_file(
            f"gpx\\{sub_folder}",
            filename,
            (hike_date.year, hike_date.month, hike_date.day)
        )
        # build_map()
    else:
        print("No new .gpx downloads found")


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


def detailed_route_plot(gpx_file: str = ""):
    """For troubleshooting purposes.  Plot a route from raw
        gpx file, showing markers for minute-by-minute position"""
    if not gpx_file:
        gpx_file = get_latest_gpx_file()
    df = df_from_gpx(f"gpx\\{gpx_file}")
    centre = tuple((df[field].mean() for field in ("latitude", "longitude")))
    m = folium.Map(location=centre, tiles=folium.TileLayer("cartodb positron", name="Detailed"), zoom_start=14)
    points = [*zip(df["longitude"], df["latitude"])]
    gj = geojson.FeatureCollection([geojson.LineString(points)])
    line = folium.GeoJson(
        gj,
        style_function=lambda ft: {"color": "blue", "opacity": 0.3, "weight": 5},
    )
    line.add_to(m)
    df_minutes = df.loc[df["time"].dt.second == 0]
    mps = [*zip(df_minutes["longitude"], df_minutes["latitude"])]
    mp_times = df_minutes["time"].dt.strftime("%H:%M").to_list()
    features = []
    for loc, time in zip(mps, mp_times):
        feature = geojson.Feature(
            geometry=geojson.Point(loc),
            properties={"time": time, "loc": loc}
        )
        features.append(feature)
    minute_markers = geojson.FeatureCollection(features)
    folium.GeoJson(
        minute_markers,
        marker=folium.Circle(radius=10, fill_color="orange", fill_opacity=0.4, color="black", weight=1),
        tooltip=folium.GeoJsonTooltip(
            fields=["time", "loc"],
            style="""font-size: 30px;"""
        ),
    ).add_to(m)
    m.save("page\\detailed.html")


def rollback(chosen_file: str = ""):
    """select a previous HikeDetails.csv file to roll back to,
        and delete and .pts files created after that date"""
    # TODO: only force choice if chosen_file not passed
    previous_files = {
        i: (file, arrow.get(int(file[:file.index(".")]), tzinfo="local"))
        for i, file in enumerate(os.listdir("Previous Hike Details"), start=1)
    }
    selected = input(
        f"Roll back to HikeDetails.csv as of:\n"
        f"{'\n'.join(f"\t[{ii}] {dd.format('ddd Do MMM HH:mm:ss')}"
                     for ii, v in previous_files.items()
                     for _, dd in [v]
                     )}\n"
    )
    if (selected.isnumeric() and
            (file_index := int(selected)) in previous_files):
        chosen_file, rollback_time = previous_files[file_index]
        os.remove("HikeDetails.csv")
        shutil.copy(
            f"Previous Hike Details\\{chosen_file}",
            "HikeDetails.csv"
        )
        # TODO: re-instate this code when going live
        # rf = "routes"
        # pts_to_remove = filter(
        #     lambda p: os.path.getmtime(f"{rf}\\{p}") >
        #               rollback_time.timestamp(),
        #     os.listdir(f"{rf}")
        # )
        # for pf in pts_to_remove:
        #     os.remove(f"{rf}\\{pf}")
    else:
        print("Invalid input")


df_stations = build_stations_df()


if __name__ == "__main__":
    my_parser = argparse.ArgumentParser(description='Map builder')
    my_parser.add_argument('Operation',
                           metavar='operation',
                           type=str,
                           help='[B] build map\n'
                                '[S] scrape meetup for new events\n'
                                '[A] add the latest hike\n'
                                '[D] plot a detailed route\n'
                                '[R] roll back to a previous state\n')
    my_parser.add_argument("-s", "--subfolder", type=int)
    args = my_parser.parse_args()
    op = args.Operation.upper()

    options = {
        "A": integrated_process,
        "B": build_map,
        "S": check_and_update_meetup_events,
        "D": detailed_route_plot,
        "R": rollback,
    }
    if op in options:
        if op == "A" and args.subfolder:
            integrated_process(args.subfolder)
        else:
            options[op]()
    else:
        print(f"{op} is not a valid operation code")
