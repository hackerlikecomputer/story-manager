# %%
from datetime import datetime
import json
import os
from pathlib import Path
import re
import pandas as pd


def load_data(s):
    f = s["project_dir"] / "stories.xlsx"
    if os.path.exists(f):
        df = pd.read_excel(f, index_col=0)
    else:
        df = pd.DataFrame({"slug": [], "start_date": [], "mtime": [], "path": []})
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


def update_existing(df, slug, start_date, mtime, path):
    index = get_index_by_slug(slug, df)
    row = {"slug": slug, "start_date": start_date, "mtime": mtime, "path": path}
    for colname in row:
        df.at[index, colname] = row[colname]
    return df


def update_data(df, slug, start_date, mtime, path):
    if record_exists(slug, df):
        df = update_existing(df, slug, start_date, mtime, path)
    else:
        df = df.append(
            {"slug": slug, "start_date": start_date, "mtime": mtime, "path": path},
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
                        start_date = get_start_date(story_dir)
                        mtime = get_mtime(s["project_dir"] / cat_dir / story_dir)
                        path = s["project_dir"] / cat_dir / story_dir
                        df = update_data(df, slug, start_date, mtime, path)
    return df


def cleanup(df):
    df["exists"] = df.path.apply(lambda dir: os.path.exists(dir))
    df = df[df.exists]
    del df["exists"]
    return df


def auto_fit_columns(df, sheet):
    df = df.reset_index()
    for i, col in enumerate(df):
        if col == "path":
            sheet.set_column(i, i, 10)
        else:
            s = df[col]
            max_len = max(s.astype(str).map(len).max(), len(str(s.name))) + 1
            sheet.set_column(i, i, max_len)


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
    save(df, s)


if __name__ == "__main__":
    _main()
