# %%
from datetime import datetime
import json
import os
from pathlib import Path
import re
import time
import pandas as pd
# from watchdog.observers import Observer
# from watchdog.events import FileSystemEventHandler
from win10toast import ToastNotifier


class StoryManagerException(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self.n = ToastNotifier()
        self.n.show_toast("Story manager threw an error!", msg)


class StoryManager:
    def __init__(self):
        with open("settings.json", "r") as f:
            self.s = json.load(f)
        self.s["project_dir"] = Path(self.s["project_dir"])

    def load_data(self):
        f = self.s["project_dir"] / "stories.xlsx"
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

    def ignore_dir(self, dirname):
        if dirname.startswith(".") or any(
            dirname.lower() == i.lower() for i in self.s["ignore_subdirs"]
        ):
            return True
        else:
            return False

    def get_story_slug(self, dirname):
        pat = re.compile(r"(?<=\d\d\d\d-\d\d-\d\d\s).*")
        m = pat.search(dirname)
        if m:
            return m.group().strip()
        else:
            raise StoryManagerException(f"Unable to get story slug from name {dirname}")

    def get_start_date(self, dirname):
        pat = re.compile(r"\d\d\d\d-\d\d-\d\d(?=\s\w+)")
        m = pat.search(dirname)
        if m:
            return m.group()
        else:
            raise StoryManagerException(f"cannot get start date from name {dirname}")

    def get_mtime(self, dir):
        return datetime.fromtimestamp(
            max(os.path.getmtime(root) for root, _, _, in os.walk(dir))
        )

    def get_status(self, dir):
        if isinstance(dir, str):
            dir = Path(dir)
        if not os.path.exists(dir / ".status"):
            raise StoryManagerException(f"Missing status file in folder {dir}")
        else:
            with open(dir / ".status", "r") as f:
                status = f.read()
            return status

    def record_exists(self, slug, df):
        if slug in df["slug"].tolist():
            return True
        else:
            return False

    def get_index_by_slug(self, slug, df):
        s = df[df["slug"] == slug].index
        if len(s) > 1:
            raise StoryManagerException(f"found multple records for folder for {slug}")
        try:
            index = s[0]
        except KeyError:
            raise StoryManagerException(f"expected index for slug {slug}")
        return index

    def update_existing(self, df, slug, category, start_date, mtime, status, path):
        index = self.get_index_by_slug(slug, df)
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

    def update_data(self, df, slug, category, start_date, mtime, status, path):
        if self.record_exists(slug, df):
            df = self.update_existing(
                df, slug, category, start_date, mtime, status, path
            )
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

    def update_all(self, df):
        for cat_dir in os.listdir(self.s["project_dir"]):
            if os.path.isdir(self.s["project_dir"] / cat_dir):
                if not self.ignore_dir(cat_dir):
                    for story_dir in os.listdir(self.s["project_dir"] / cat_dir):
                        if not self.ignore_dir(story_dir):
                            slug = self.get_story_slug(story_dir)
                            category = cat_dir
                            start_date = self.get_start_date(story_dir)
                            mtime = self.get_mtime(
                                self.s["project_dir"] / cat_dir / story_dir
                            )
                            status = self.get_status(
                                self.s["project_dir"] / cat_dir / story_dir
                            )
                            path = self.s["project_dir"] / cat_dir / story_dir
                            df = self.update_data(
                                df, slug, category, start_date, mtime, status, path
                            )
        return df

    def cleanup(self, df):
        df["exists"] = df.path.apply(lambda dir: os.path.exists(dir))
        df = df[df.exists]
        del df["exists"]
        return df

    def auto_fit_columns(self, df, sheet):
        for i, col in enumerate(df.columns):
            i = i + 1
            if col == "path":
                sheet.set_column(i, i, 10)
            else:
                s = df[col]
                max_len = max(s.astype(str).map(len).max(), len(str(s.name))) + 1
                sheet.set_column(i, i, max_len)

    def sort(self, df):
        df = df.sort_values("mtime", ascending=True).reset_index(drop=True)
        return df

    def save(self, df):
        writer = pd.ExcelWriter(
            self.s["project_dir"] / "stories.xlsx", engine="xlsxwriter"
        )
        df.to_excel(writer, sheet_name="active", index=True)
        sheet = writer.sheets["active"]
        self.auto_fit_columns(df, sheet)
        writer.save()

    def run(self):
        df = self.load_data()
        df = self.update_all(df)
        df = self.cleanup(df)
        df = self.sort(df)
        self.save(df)


def _main():
    story_manager = StoryManager()
    story_manager.run()


if __name__ == "__main__":
    _main()
