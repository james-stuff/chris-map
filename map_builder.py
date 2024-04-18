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


def build_map(from_existing_csv: bool = False):
    """run every time new hike(s) are added"""
    df_hikes = None
    if from_existing_csv:
        df_hikes = pd.read_csv("HikeDetails.csv", parse_dates=[0])
    else:
        df_hikes = generate_hike_details_for_map(
            cumulatively_find_gpx_files(
                all_known_hikes()
            ).dropna(subset="GPX")
        )

    m = folium.Map(location=(51.5, -0.15), tiles="cartodb positron", zoom_start=9)
    fg_by_year = {year: folium.FeatureGroup(name=f"{year}")
                  for year in pd.unique(df_hikes["Date"].dt.year)}    #.apply(lambda d: d[:4]))}
    walks_on_map = 0
    for ind in df_hikes.index:
        hike_data = df_hikes.loc[ind].to_dict()
        year_fg = fg_by_year[hike_data["Date"].year]
        make_line(hike_data).add_to(year_fg)
        walks_on_map += 1

    open_div = (f"<div style= 'font-family:Helvetica, Avenir, Helvetica neue, Sans-serif;"
                f"font-size:12pt;color:Black; outline:2px black; background-color:white;'>")
    folium.Marker((50.98, -1.35), icon=folium.DivIcon(
        html=f"{open_div}{walks_on_map} hikes plotted</div>",
        icon_size=(130, 20),
    )).add_to(m)

    for yfg in fg_by_year.values():
        yfg.add_to(m)
    m.add_child(folium.LayerControl(position='topright', collapsed=False, autoZIndex=True))

    map_file = "page\\map.html"
    m.save(map_file)


def make_line(hike_data: dict) -> folium.GeoJson:
    """create GeoJson feature for the route to be added to the map"""
    with open(f"routes\\{hike_data['URL']}.pts", "r") as file:
        points = [eval(ln) for ln in file.read().split("\n")]
    date = arrow.get(hike_data["Date"])
    dist_mls, dist_kms = (hike_data["Distance"] / factor for factor in (1_609, 1_000))
    tooltip = (f"{date.format('ddd Do MMM YYYY')}<br/>"
               f"{hike_data['Title']}<br/>"
               f"{route_description(hike_data)}<br/>"
               f"{dist_mls:.1f} miles / {dist_kms:.1f} km")
    gj = geojson.FeatureCollection([geojson.LineString(points)])
    return folium.GeoJson(
        gj,
        style_function=lambda feature: {"color": "blue", "opacity": 0.3, "weight": 5},
        highlight_function=lambda feature: {"color": "red", "opacity": 1.0, "weight": 3},
        tooltip=tooltip
    )


def route_description(data: dict) -> str:
    start, end = (data[k] for k in ("Start", "End"))
    if start == end:
        return f"Circular walk from {start}"
    return f"{start} to {end}"


def generate_hike_details_for_map(df_details: pd.DataFrame) -> pd.DataFrame:
    """produce from scratch the DataFrame containing all necessary details
        for all plottable hikes, and saves to a new .csv file"""
    df_stations = build_stations_df()
    starts, ends, distances = ([] for _ in range(3))
    hikes_added = 0
    for i_hike in df_details.index:
        hike_info = df_details.loc[i_hike].to_dict()
        points = extract_hike_points(hike_info["GPX"], save_to_url=hike_info["URL"])
        starts.append(find_proximate_station(points[0], df_stations))
        ends.append(find_proximate_station(points[-1], df_stations))
        distances.append(get_total_distance(points))
        hikes_added += 1
        if not (hikes_added % 10):
            print(f"{hikes_added=}")
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


def extract_hike_points(gpx_file: str, reduce_points_to: int = 500,
                        save_to_url: str = "") -> [geo.Location]:
    """translate a .gpx file in gpxpy recognised format into a reduced
        list of points.  If a 'url' is specified, will save a new .pts
        file with that name if one doesn't already exist in routes folder"""
    gpx = gpxpy.parse(open(gpx_file, encoding="utf-8"))
    gpx.reduce_points(max_points_no=reduce_points_to)
    assert len(gpx.tracks) == 1
    assert len(gpx.tracks[0].segments) == 1
    points = gpx.tracks[0].segments[0].points
    if save_to_url:
        folder, filename = "routes", f"{save_to_url}.pts"
        if filename not in os.listdir(folder):
            with open(f"{folder}\\{filename}", "w") as file:
                file.write("\n".join(
                    f"({pt.longitude}, {pt.latitude})"
                    for pt in points
                ))
    return points


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
    """Returns a dated list of gpx files found in each sub-folder under main gpx folder"""
    folder_address = f"gpx\\{provider}\\"
    file_data = []
    suunto_date_pattern = r"\d{4}-\d{2}-\d{2}"
    for gpx_file in os.listdir(folder_address):
        if gpx_file[-4:] == ".gpx":
            if "suuntoapp-" in gpx_file:
                time = re.search(suunto_date_pattern, gpx_file).group()
            else:
                with open(f"{folder_address}{gpx_file}", "r", encoding="utf-8") as gf:
                    gpx_text = gf.read()
                    time = re.search("<time>.+</time>", gpx_text).group()[6:16]
            file_data.append(
                [
                    pd.Timestamp(arrow.get(time).date()),
                    f"{folder_address}{gpx_file}"
                ]
            )
    return pd.DataFrame(file_data, columns=["Date", "GPX"])


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


if __name__ == "__main__":
    build_map()