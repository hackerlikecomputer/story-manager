from datetime import datetime
import json
import os
from pathlib import Path
import re
import time
import pandas as pd


def load_data(s):
    f = s["project_dir"] / "stories.xlsx"
    if os.path.exists(f):
        status = "not loaded"
        while status == "not loaded":
            try:
                df = pd.read_excel(f, index_col=0)
            except PermissionError:
                time.sleep(10)
            else:
                status = "loaded"
    else:
        df = pd.DataFrame(
            {
                "slug": [],
                "category": [],
                "start_date": [],
                "mtime": [],
                "status": [],
                "path": [],
            }
        )

    return df


def load_settings():
    with open("settings.json", "r") as f:
        s = json.load(f)
    s["project_dir"] = Path(s["project_dir"])
    return s


def ignore_dir(dirname, s):
    if dirname.startswith(".") or any(
        dirname.lower() == i.lower() for i in s["ignore_subdirs"]
    ):
        return True
    else:
        return False


def get_story_slug(dirname):
    pat = re.compile(r"(?<=\d\d\d\d-\d\d-\d\d\s).*")
    m = pat.search(dirname)
    if m:
        return m.group().strip()
    else:
        raise ValueError(f"Unable to get story slug from name {dirname}")


def get_start_date(dirname):
    pat = re.compile(r"\d\d\d\d-\d\d-\d\d(?=\s\w+)")
    m = pat.search(dirname)
    if m:
        return m.group()
    else:
        raise ValueError(f"cannot get start date from name {dirname}")


def get_mtime(dir):
    return datetime.fromtimestamp(
        max(os.path.getmtime(root) for root, _, _, in os.walk(dir))
    )


def get_status(dir):
    if isinstance(dir, str):
        dir = Path(dir)
    if not os.path.exists(dir / ".status"):
        raise ValueError(f"Missing status file in folder {dir}")
    else:
        with open(dir / ".status", "r") as f:
            status = f.read()
        return status


def record_exists(slug, df):
    if slug in df["slug"].tolist():
        return True
    else:
        return False


def get_index_by_slug(slug, df):
    s = df[df["slug"] == slug].index
    if len(s) > 1:
        raise ValueError(f"found multple records for folder for {slug}")
    try:
        index = s[0]
    except KeyError:
        raise KeyError(f"expected index for slug {slug}")
    return index


def update_existing(df, slug, category, start_date, mtime, status, path):
    index = get_index_by_slug(slug, df)
    row = {
        "slug": slug,
        "category": category,
        "start_date": start_date,
        "mtime": mtime,
        "status": status,
        "path": path,
    }
    for colname in row:
        df.at[index, colname] = row[colname]
    return df


def update_data(df, slug, category, start_date, mtime, status, path):
    if record_exists(slug, df):
        df = update_existing(df, slug, category, start_date, mtime, status, path)
    else:
        df = df.append(
            {
                "slug": slug,
                "category": category,
                "start_date": start_date,
                "mtime": mtime,
                "status": status,
                "path": path,
            },
            ignore_index=True,
        )
    return df


def update_all(df, s):
    for cat_dir in os.listdir(s["project_dir"]):
        if os.path.isdir(s["project_dir"] / cat_dir):
            if not ignore_dir(cat_dir, s):
                for story_dir in os.listdir(s["project_dir"] / cat_dir):
                    if not ignore_dir(story_dir, s):
                        slug = get_story_slug(story_dir)
                        category = cat_dir
                        start_date = get_start_date(story_dir)
                        mtime = get_mtime(s["project_dir"] / cat_dir / story_dir)
                        status = get_status(s["project_dir"] / cat_dir / story_dir)
                        path = s["project_dir"] / cat_dir / story_dir
                        df = update_data(
                            df, slug, category, start_date, mtime, status, path
                        )
    return df


def cleanup(df):
    df["exists"] = df.path.apply(lambda dir: os.path.exists(dir))
    df = df[df.exists]
    del df["exists"]
    return df


def auto_fit_columns(df, sheet):
    for i, col in enumerate(df.columns):
        i = i + 1
        if col == "path":
            sheet.set_column(i, i, 10)
        else:
            s = df[col]
            max_len = max(s.astype(str).map(len).max(), len(str(s.name))) + 1
            sheet.set_column(i, i, max_len)


def sort(df):
    df = df.sort_values("mtime", ascending=True).reset_index(drop=True)
    return df


def save(df, s):
    writer = pd.ExcelWriter(s["project_dir"] / "stories.xlsx", engine="xlsxwriter")
    df.to_excel(writer, sheet_name="active", index=True)
    sheet = writer.sheets["active"]
    auto_fit_columns(df, sheet)
    writer.save()


def _main():
    s = load_settings()
    df = load_data(s)
    df = update_all(df, s)
    df = cleanup(df)
    df = sort(df)
    save(df, s)


if __name__ == "__main__":
    _main()
