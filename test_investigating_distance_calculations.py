import map_builder as mb
import pandas as pd
import arrow
import gpxpy
from gpxpy import geo
import folium
import geojson


def build_map(route_data: [[]]):
    m = folium.Map(location=(51.5, -0.15), tiles=folium.TileLayer("cartodb positron", name="Clear"), zoom_start=9)
    for hike in route_data:
        for line in hike:
            line.add_to(m)

    map_title = f"Test map"
    title_html = (f'<h4 style="position:fixed;z-index:100000;bottom:5px;left:20px;background-color:white;" >'
                  f'{map_title}</h4>')
    m.get_root().html.add_child(folium.Element(title_html))

    map_file = "page\\test_map.html"
    m.save(map_file)


def make_all_lines(hike_data: dict) -> [folium.GeoJson]:
    with open(hike_data["GPX"], encoding="utf-8") as gpx_file:
        gpx = gpxpy.parse(gpx_file)
        gpx_points = len(gpx.tracks[0].segments[0].points)
    resolutions = [500, gpx_points // 10, 1_000_000]
    colours = ["blue", "cyan", "green",]
    date = arrow.get(hike_data["Date"])
    base_tooltip = (f"{date.format('ddd Do MMM YYYY')}<br/>"
                    f"{hike_data['Title']}<br/>"
                    f"{mb.route_description(hike_data)}<br/>")
    lines = []
    for r, c in zip(resolutions, colours):
        print(f"Drawing route for {hike_data['Title']} with {r} points . . .")
        points = [
            geo.Location(pt.latitude, pt.longitude) for pt in
            mb.gpxpy_points_from_gpx_file(hike_data["GPX"], r)
            ]
        # Todo: currently second argument to mb.gpxpy_points_from_gpx_file() is ignored
        tooltip = base_tooltip + (f"{mb.distance_description(mb.get_total_distance(points))}"
                                  f"<br>{r} points plotted<br>GPX file has {gpx_points} points")
        lines.append(make_line(points, tooltip, c))
    return lines


def make_line(points: [geo.Location], tooltip: str, colour: str) -> folium.GeoJson:
    """create GeoJson feature for the route to be added to the map"""
    points = [(pt.longitude, pt.latitude) for pt in points]
    gj = geojson.FeatureCollection([geojson.LineString(points)])
    return folium.GeoJson(
        gj,
        style_function=lambda feature: {"color": colour, "opacity": 0.3, "weight": 5},
        highlight_function=lambda feature: {"color": "red", "opacity": 1.0, "weight": 3},
        tooltip=tooltip
    )


def count_pts(url: str) -> int:
    """NB. only used in Python console so far"""
    with open(f"routes\\{url}.pts") as file:
        contents = file.read()
        return contents.count(",")


def count_points_in_gpx_file(file: str):
    """NB. only used in Python console so far"""
    with open(file, encoding="utf-8") as gpx_file:
        gpx = gpxpy.parse(gpx_file)
        return len(gpx.tracks[0].segments[0].points)


"""
Creating a full map without reducing points 
(well, setting max_points_no=1_000_000):
    - map.html is over 62MB in size
    - takes 5-10sec to load
    - performance is indeed awful with at least a second or two lag 
        when going up or down a zoom level
    - .pts files are mostly around 400-500kB in size
    
Settled on 8,000 as the number of points above which
    to start dividing by 10 for the sampling.
    Most of my manually-created files fall below
    this limit, as well as most of 07's and 03's
"""


def test_run():
    """Take several hikes, and for each one plot versions of it
            with differing numbers of sample points"""
    # TODO:
    #   no. of points // 10 seems to give much truer results
    #   Consider using a different number of points to plot the line, than to calculate the distance
    hikes = [
        # "Aclea",
        # "Deer by the River",
        "Grantchester",
        "Byrhtnoth",
        # "Christmas Party ",
    ]
    df = pd.read_csv("HikeDetails.csv", parse_dates=[0])
    hike_data = [
        make_all_lines(df.loc[df["Title"].str.contains(h)].iloc[0].to_dict())
        for h in hikes
    ]
    build_map(hike_data)
