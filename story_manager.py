# %%
from datetime import datetime
import json
import os
from pathlib import Path
import re
import time
import warnings
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from win10toast import ToastNotifier
from xlsxwriter.exceptions import FileCreateError


class StoryManagerWarning(Warning):
    """sends warnings to windows 10 toast notifications"""

    def __init__(self, msg):
        super().__init__(msg)
        self.n = ToastNotifier()
        self.n.show_toast("Story Manager Warning", msg)


class StoryManagerException(Exception):
    """sends errors to windows 10 toast notification"""

    def __init__(self, msg):
        super().__init__(msg)
        self.n = ToastNotifier()
        self.n.show_toast("Story manager threw an error!", msg)


class StoryManager:
    """watches project directory for changes and updates stories spreadsheet"""

    def __init__(self):
        with open("settings.json", "r") as f:
            self.s = json.load(f)
        self.s["project_dir"] = Path(self.s["project_dir"])

    def load_data(self):
        """loads the existing data, or creates an empty DataFrame"""

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
        """
        whether to ignore the directory.
        will ignore all directories starting with "." or that are explicitly ignored
        in settings.json

        args:
            dirname (str): name of directory to check

        returns: bool
        """

        if dirname.startswith(".") or any(
            dirname.lower() == i.lower() for i in self.s["ignore_subdirs"]
        ):
            return True
        else:
            return False

    def get_story_slug(self, dirname):
        """get the story slug from the directory name

        args:
            dirname (str): name of directory to get name from

        returns: str
        """

        pat = re.compile(r"(?<=\d\d\d\d-\d\d-\d\d\s).*")
        m = pat.search(dirname)
        if m:
            slug = m.group().strip()
        else:
            slug = None
            warnings.warn(
                f"Unable to get story slug from name {dirname}", StoryManagerWarning
            )
        return slug

    def get_start_date(self, dirname):
        """
        get the start date of the story from the folder name

        args:
            dirname (str): name of directory to check

        returns: str
        """

        pat = re.compile(r"\d\d\d\d-\d\d-\d\d(?=\s\w+)")
        m = pat.search(dirname)
        if m:
            return m.group()
        else:
            raise StoryManagerException(f"cannot get start date from name {dirname}")

    def get_mtime(self, dir):
        """
        recursively get latest modified time of any file in folder

        args:
            dir (str): full path to directory

        returns: datetime object
        """

        return datetime.fromtimestamp(
            max(os.path.getmtime(root) for root, _, _, in os.walk(dir))
        )

    def get_status(self, dir):
        """
        gets the status from a .status file

        args:
            dir (str): full path to directory

        returns: str
        """

        if isinstance(dir, str):
            dir = Path(dir)
        if not os.path.exists(dir / ".status"):
            status = None
        else:
            with open(dir / ".status", "r") as f:
                status = f.read()
            return status

    def record_exists(self, slug, df):
        """
        check if the story is present in spreadsheet

        args:
            slug (str): story slug to check
            df (DataFrame): data to draw from

        returns: bool
        """

        if slug in df["slug"].tolist():
            return True
        else:
            return False

    def get_index_by_slug(self, slug, df):
        """
        get the dataframe index given a story slug

        args:
            slug (str): slug to check
            df (DataFrame): data to check in

        returns: int
        """

        s = df[df["slug"] == slug].index
        if len(s) > 1:
            raise StoryManagerException(f"found multple records for folder for {slug}")
        try:
            index = s[0]
        except KeyError:
            raise StoryManagerException(f"expected index for slug {slug}")
        return index

    def update_existing(self, df, slug, category, start_date, mtime, status, path):
        """
        updates an existing record

        args:
            df (DataFrame): data to update
            slug (str): story slug
            category (str): story category
            start_date (str): start date from folder name
            mtime (datetime): last modified date
            status (str): story status
            path (str): path to story dir

        returns: DataFrame
        """

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
        """
        updates a record if it exists, or appends a new one

        args:
            df (DataFrame): data to update
            slug (str): story slug
            category (str): story category
            start_date (str): start date from folder name
            mtime (datetime): last modified date
            status (str): story status
            path (str): path to story dir

        returns: DataFrame
        """

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
        """
        updates all records

        args:
            df (DataFrame): data to update

        returns: DataFrame
        """

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
        """
        drops records for folders that no longer exist

        args:
            df (DataFrame): dataframe to clean up

        returns: DataFrame
        """

        df["exists"] = df.path.apply(lambda dir: os.path.exists(dir))
        df = df[df.exists]
        del df["exists"]
        return df

    def auto_fit_columns(self, df, sheet):
        """
        fits the columns to the size of their contents

        args:
            df (DataFrame): data to get sizes from
            sheet (xlsxwriter sheet object): sheet to modify

        returns: None
        """

        for i, col in enumerate(df.columns):
            i = i + 1
            if col == "path":
                sheet.set_column(i, i, 10)
            else:
                s = df[col]
                max_len = max(s.astype(str).map(len).max(), len(str(s.name))) + 1
                sheet.set_column(i, i, max_len)

    def sort(self, df):
        """
        sort dataframe by mtime

        args:
            df (DataFrame): dataframe to sort

        returns: DataFrame
        """

        df = df.sort_values("mtime", ascending=True).reset_index(drop=True)
        return df

    def save(self, df):
        """
        save and format excel file

        args:
            df (DataFrame): dataframe to save

        returns: None
        """

        n_retries = 0
        while n_retries < 10:
            try:
                writer = pd.ExcelWriter(
                    self.s["project_dir"] / "stories.xlsx", engine="xlsxwriter"
                )
                df.to_excel(writer, sheet_name="active", index=True)
                sheet = writer.sheets["active"]
                self.auto_fit_columns(df, sheet)
                writer.save()
                break
            except FileCreateError:
                warnings.warn("trying to save but file is open", StoryManagerWarning)
                n_retries += 1
                time.sleep(5)
        else:
            raise StoryManagerException(
                f"Unable to save file. Please close the file and try again"
            )

    def run(self):
        df = self.load_data()
        df = self.update_all(df)
        df = self.cleanup(df)
        df = self.sort(df)
        self.save(df)


class Handler(FileSystemEventHandler):
    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None
        elif event.event_type in ["moved", "deleted", "created", "modified"]:
            manager = StoryManager()
            manager.run()


class Watcher:
    def __init__(self):
        self.watch_directory = (
            "C:\\Users\\cmhack0114\\OneDrive - CBS Corporation\\Stories\\Active"
        )
        self.observer = Observer()

    def run(self):
        handler = Handler()
        self.observer.schedule(handler, self.watch_directory, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except Exception:
            self.observer.stop()
            raise StoryManagerException(f"Story manager crashed")
        self.observer.join()


def _main():
    watcher = Watcher()
    watcher.run()


if __name__ == "__main__":
    _main()
