# /// script
# requires-python = ">=3.11"
# dependencies = ["geopandas>=1", "pyogrio>=0.8", "pandas>=2", "pyproj>=3.6", "shapely>=2"]
# ///
"""Regenerate the Minnesota lakes dataset bundled with the JupyterLite site.

Writes two small CSVs consumed by the SciPy lightning-talk notebook
(demos/jupyterlite/files/00-scipy-lightning.ipynb):

- ``demos/jupyterlite/files/data/mn_lakes.csv`` — one row per DNR-numbered
  lake basin of 10+ acres (name, lat, lon, acres), extracted from the
  authoritative MN DNR Hydrography GeoPackage on the Minnesota Geospatial
  Commons (https://gisdata.mn.gov/dataset/water-dnr-hydrography).
  Filter: ``wb_class`` in a lake-like set, main basins only
  (``sub_flag != 'Y'``), a valid DNR lake number (``dowlknum``), not
  entirely outside Minnesota, ``acres >= 10``. The classic DNR statistic is
  11,842 lakes of 10+ acres; today's hydrography delineations hold more —
  the notebook quotes both, and this script prints the extracted count.
- ``demos/jupyterlite/files/data/mn_outline.csv`` — the Minnesota state
  outline (lon, lat vertex rows, blank rows separating rings) from the US
  Census 1:20,000,000 cartographic boundary file, for plot context.

Both CSVs are committed; rerun only to refresh from new source data:

    uv run scripts/make-mn-lakes-data.py

Source archives (~200 MB for hydrography) download to --cache-dir (default
~/.cache/mn-lakes-data) and are reused on later runs.
"""

from __future__ import annotations

import argparse
import io
import sys
import urllib.request
import zipfile
from pathlib import Path

HYDRO_URL = (
    "https://resources.gisdata.mn.gov/pub/gdrs/data/pub/us_mn_state_dnr/"
    "water_dnr_hydrography/gpkg_water_dnr_hydrography.zip"
)
STATES_URL = "https://www2.census.gov/geo/tiger/GENZ2023/shp/cb_2023_us_state_20m.zip"

HYDRO_LAYER = "dnr_hydro_features_all"

#: ``wb_class`` values that count as lakes. Everything else in the layer is
#: wetland, riverine, island, drained, or industrial-pond classes.
LAKE_CLASSES = frozenset(
    {
        "Lake or Pond",
        "Reservoir",
        "Mine Pit Lake",
        "Mine Pit Lake (NF)",
    }
)

MIN_ACRES = 10.0


def fetch(url: str, dest: Path) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        print(f"using cached {dest}")
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"downloading {url} -> {dest}")
    with urllib.request.urlopen(url) as response, open(dest, "wb") as out:
        while chunk := response.read(1 << 20):
            out.write(chunk)
    return dest


def extract_member(zip_path: Path, suffix: str, workdir: Path) -> Path:
    with zipfile.ZipFile(zip_path) as archive:
        for name in archive.namelist():
            if name.endswith(suffix):
                archive.extract(name, workdir)
                return workdir / name
    raise FileNotFoundError(f"no *{suffix} member in {zip_path}")


def write_lakes_csv(gpkg: Path, out_csv: Path) -> None:
    import pandas as pd
    import pyogrio
    from pyproj import Transformer

    df = pyogrio.read_dataframe(
        gpkg,
        layer=HYDRO_LAYER,
        read_geometry=False,
        columns=[
            "dowlknum",
            "pw_basin_name",
            "sub_flag",
            "wb_class",
            "acres",
            "center_utm_x",
            "center_utm_y",
            "outside_mn",
        ],
    )
    lakes = df[
        df["wb_class"].isin(LAKE_CLASSES)
        & (df["sub_flag"] != "Y")
        & df["dowlknum"].notna()
        & (df["dowlknum"] != "00000000")
        & (df["outside_mn"] != "Y")
        & (df["acres"] >= MIN_ACRES)
        & df["center_utm_x"].notna()
        & df["center_utm_y"].notna()
    ].copy()

    # Native CRS is UTM 15N (EPSG:26915); the notebook wants plain lat/lon.
    transformer = Transformer.from_crs(26915, 4326, always_xy=True)
    lon, lat = transformer.transform(
        lakes["center_utm_x"].to_numpy(), lakes["center_utm_y"].to_numpy()
    )

    out = pd.DataFrame(
        {
            "name": lakes["pw_basin_name"].fillna("").str.replace(",", ";"),
            "lat": [round(v, 5) for v in lat],
            "lon": [round(v, 5) for v in lon],
            "acres": lakes["acres"].round(1),
        }
    ).sort_values("acres", ascending=False)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)
    print(
        f"wrote {out_csv}: {len(out):,} lake basins >= {MIN_ACRES:g} acres "
        f"({out_csv.stat().st_size / 1024:.0f} KiB)"
    )


def write_outline_csv(states_zip: Path, out_csv: Path, workdir: Path) -> None:
    import pyogrio
    from shapely.geometry import MultiPolygon

    shp = extract_member(states_zip, ".shp", workdir)
    for suffix in (".shx", ".dbf", ".prj"):
        extract_member(states_zip, suffix, workdir)
    states = pyogrio.read_dataframe(shp)
    minnesota = states[states["STUSPS"] == "MN"]
    if len(minnesota) != 1:
        raise RuntimeError(f"expected exactly one MN feature, got {len(minnesota)}")
    geometry = minnesota.geometry.iloc[0]
    polygons = list(geometry.geoms) if isinstance(geometry, MultiPolygon) else [geometry]

    lines = ["lon,lat"]
    for i, polygon in enumerate(polygons):
        if i:
            lines.append(",")  # ring separator -> NaN row under np.genfromtxt
        lines.extend(f"{x:.4f},{y:.4f}" for x, y in polygon.exterior.coords)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_csv.write_text("\n".join(lines) + "\n")
    total = sum(len(polygon.exterior.coords) for polygon in polygons)
    print(f"wrote {out_csv}: {total} outline vertices in {len(polygons)} ring(s)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path.home() / ".cache" / "mn-lakes-data",
        help="where source archives are downloaded and unpacked",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent
        / "demos"
        / "jupyterlite"
        / "files"
        / "data",
        help="output directory for the CSVs",
    )
    args = parser.parse_args()

    hydro_zip = fetch(HYDRO_URL, args.cache_dir / Path(HYDRO_URL).name)
    states_zip = fetch(STATES_URL, args.cache_dir / Path(STATES_URL).name)
    gpkg = extract_member(hydro_zip, ".gpkg", args.cache_dir)

    write_lakes_csv(gpkg, args.out_dir / "mn_lakes.csv")
    write_outline_csv(states_zip, args.out_dir / "mn_outline.csv", args.cache_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
