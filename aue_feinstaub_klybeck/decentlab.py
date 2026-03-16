# MIT License
#
# Copyright (c) 2016 Decentlab GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import copy
import json
import re
import warnings

import pandas as pd
import requests


def query(
    domain,
    api_key,
    time_filter="",
    device="//",
    location="//",
    sensor="//",
    include_network_sensors=False,
    channel="//",
    agg_func=None,
    agg_interval=None,
    do_unstack=True,
    convert_timestamp=True,
    timezone="UTC",
    with_location=False,
    database="main",
):

    select_var = "value"
    fill = ""
    interval = ""

    if agg_func is not None:
        select_var = agg_func + '("value") as value'
        fill = "fill(null)"

    if agg_interval is not None:
        interval = ", time({})".format(agg_interval)

    if time_filter != "":
        time_filter = " AND " + time_filter

    filter_ = (" location =~ {} AND node =~ {} AND sensor =~ {} AND ((channel =~ {} OR channel !~ /.+/) {})").format(
        location, device, sensor, channel, ("" if include_network_sensors else "AND channel !~ /^link-/")
    )

    q = ('SELECT {} FROM "measurements"  WHERE {} {} GROUP BY channel,node,sensor,unit{},uqk,title {} {}').format(
        select_var, filter_, time_filter, ",location" if with_location else "", interval, fill
    )

    URL = "https://{}/api/datasources/proxy/uid/{}/query".format(domain, database)
    r = requests.get(
        URL, params={"db": "main", "epoch": "ms", "q": q}, headers={"Authorization": "Bearer {}".format(api_key)}
    )

    data = json.loads(r.text)

    if "results" not in data or "series" not in data["results"][0]:
        raise ValueError("No series returned: {}".format(r.text))

    def _ix2df(series):
        df = pd.DataFrame(series["values"], columns=series["columns"])
        df["series"] = series["tags"]["uqk"]
        if with_location:
            df["location"] = series["tags"]["location"]
        return df, (series["tags"]["uqk"], series["tags"])

    series, tags = zip(*(_ix2df(s) for r in data["results"] for s in r["series"]))

    df = pd.concat(series)
    tags = dict(tags)

    if convert_timestamp:
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        try:
            df["time"] = df["time"].dt.tz_localize("UTC")
        except TypeError:
            pass
        df["time"] = df["time"].dt.tz_convert(timezone)

    indices = ["time", "series"]
    if with_location:
        indices.append("location")
    df = df.set_index(indices)
    df = df.sort_index()

    if do_unstack:
        df = df.unstack(level="series")
        if with_location:
            df = df.unstack(level="location")
        df.columns = df.columns.droplevel(0)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        setattr(df, "tags", tags)

    return df


def add_column_tags(src_df, src_col_name, dest_sensor, dest_unit=None, dest_df=None):
    tags = getattr(src_df, "tags")

    new_tags = [tags[src_col_name]["node"], dest_sensor]
    if "channel" in tags[src_col_name]:
        new_tags.append(tags[src_col_name]["channel"])
    dest_col_name = ".".join(new_tags)

    tags[dest_col_name] = copy.deepcopy(tags[src_col_name])
    tags[dest_col_name]["sensor"] = dest_sensor
    tags[dest_col_name]["uqk"] = dest_col_name
    tags[dest_col_name]["title"] = dest_sensor if dest_unit is None else "{} [{}]".format(dest_sensor, dest_unit)
    if dest_unit is not None:
        tags[dest_col_name]["unit"] = dest_unit
    if "channel" in tags[src_col_name]:
        tags[dest_col_name]["channel"] = tags[src_col_name]["channel"]

    if dest_df is not None:
        setattr(dest_df, "tags", tags)

    return dest_col_name


def store(domain, api_key, df, do_stack=True, convert_timestamp=True, database="processed", device_suffix=""):

    tags = getattr(df, "tags")

    if convert_timestamp:
        names = df.index.name
        df = df.reset_index()
        df["time"] = df["time"].astype(pd.np.int64)
        df = df.set_index(names)

    if do_stack:
        df = pd.DataFrame(df.stack())

    df = df.dropna()

    datapoints = (
        "measurements,{tags} value={value} {time}000000".format(
            value=val.values.item(),
            tags=",".join("{}={}".format(k, re.sub(r"([\ =,])", r"\\\1", v)) for k, v in tags[val.name[1]].items()),
            time=val.name[0],
        )
        for idx, val in df.iterrows()
    )

    URL = "https://{}/api/datasources/proxy/uid/{}/write".format(domain, database)
    r = requests.post(
        URL,
        params={"db": database},
        data="\n".join(datapoints).encode("utf-8"),
        headers={"Authorization": "Bearer {}".format(api_key)},
    )
    if not r.ok:
        raise ValueError("HTTP Post error: {}".format(r.text))
