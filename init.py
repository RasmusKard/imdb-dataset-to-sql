"""Textual config wizard that generates a config module under `configs/`.

Run with `uv run init.py`. Every column's include flag, SQL name and dtype is
edited on one screen; `Generate config` previews the file and writes it after
confirmation. The generated file is plain Python with the same shape as
`configs/default.py`, so `CONFIG=<name> uv run main.py` picks it up without any
other changes.
"""

import copy
import re
from pathlib import Path

from rich.syntax import Syntax
from sqlalchemy import types as sqltypes
from textual.app import App, ComposeResult
from textual.containers import Horizontal, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Button, Checkbox, Input, Label, Static

from modules.const import IMDB_DATA_ALLOWED_COLUMNS
from modules.settings_parsers import get_settings_tables_validity

ALL_COLUMNS = list(IMDB_DATA_ALLOWED_COLUMNS)

SNAKE_CASE_DEFAULTS = {
    "tconst": "tconst",
    "titleType": "title_type",
    "primaryTitle": "primary_title",
    "originalTitle": "original_title",
    "isAdult": "is_adult",
    "startYear": "start_year",
    "endYear": "end_year",
    "runtimeMinutes": "runtime_minutes",
    "genres": "genres",
    "averageRating": "average_rating",
    "numVotes": "num_votes",
}

# Characters that break when the name is used as a python module name or in
# `CONFIG=<name>`; everything else is passed through untouched.
CONFIG_NAME_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")

# Dtype overrides are accepted as `Name` or `Name(n)` only, where `Name` is one
# of these SQLAlchemy types. No dynamic evaluation is involved.
DTYPE_NAMES = (
    "String",
    "Text",
    "Integer",
    "SmallInteger",
    "BigInteger",
    "Float",
    "Numeric",
    "Boolean",
    "Date",
    "DateTime",
    "Unicode",
    "UnicodeText",
)
DTYPE_PATTERN = re.compile(rf"^({'|'.join(DTYPE_NAMES)})(?:\(\s*(\d+)?\s*\))?$")


class ValidationError(Exception):
    pass


def parse_dtype(raw):
    """Parse a user typed dtype like `String(400)` or `Text`.

    Returns the instance so its repr round-trips into the config file.
    """
    match = DTYPE_PATTERN.match(raw.strip())
    if not match:
        raise ValidationError(
            f"`{raw.strip()}` is not a valid dtype. Use one of {', '.join(DTYPE_NAMES)} with an optional `(n)`, e.g. `String(400)`"
        )
    name, length = match.groups()
    cls = getattr(sqltypes, name)
    return cls(int(length)) if length is not None else cls()


def validate_config_name(name, configs_dir):
    if not CONFIG_NAME_PATTERN.match(name):
        raise ValidationError(
            "Config name must be lowercase letters, digits and underscores, starting with a letter"
        )
    if (configs_dir / f"{name}.py").exists():
        raise FileExistsError(f"`configs/{name}.py` already exists")


def build_config(answers):
    """Assemble the config dict from wizard answers, mirroring configs/default.py."""
    selected = list(answers["columns"])
    values = {col: answers["renames"][col] for col in selected}

    if answers["is_split_genres"] and "genres" in values:
        del values["genres"]

    rename_to = set(values.values())
    if len(rename_to) != len(values):
        raise ValidationError("Two columns renamed to the same target name")

    dtypes = {}
    for col, dtype in answers.get("dtype_overrides", {}).items():
        if col in values and dtype is not None:
            dtypes[answers["renames"][col]] = dtype

    tables = {
        answers["title_table_name"]: {"values": values},
    }
    if dtypes:
        tables[answers["title_table_name"]]["dtypes"] = dtypes
    if answers["is_split_genres"]:
        tables[answers["genres_table_name"]] = {
            "values": {"tconst": answers["renames"]["tconst"], "genres": "genres"}
        }

    settings = {
        "is_split_genres_into_reftable": answers["is_split_genres"],
        "is_convert_title_type_str_to_int": "titleType" in values
        and answers["is_convert_title_type"],
        "is_remove_adult": answers["is_remove_adult"],
        "is_streaming": answers["is_streaming"],
        "is_ignore_db_has_tables_error": answers["is_ignore_db_has_tables_error"],
        "is_batching": answers["is_batching"],
        "batch_count": answers["batch_count"],
    }
    if answers.get("blocked_titletypes"):
        settings["blocked_titletypes"] = sorted(answers["blocked_titletypes"])
    if answers.get("blocked_genres"):
        settings["blocked_genres"] = sorted(answers["blocked_genres"])

    columns_to_drop = set(ALL_COLUMNS) - set(selected)
    settings["columns_to_drop"] = sorted(columns_to_drop)

    # No guards needed for `isAdult`/`titleType`/`genres` versus `columns_to_drop`:
    # `apply_title_cleaners` filters on them before dropping them in the same pass,
    # which is exactly how configs/default.py does it.

    config = {"tables": tables, "settings": settings}

    # Reuse the runtime validator so a config that fails here never reaches disk.
    # It mutates the dict it is given (fills in default dtypes), so hand it a copy.
    try:
        get_settings_tables_validity(
            copy.deepcopy(config["tables"]), IMDB_DATA_ALLOWED_COLUMNS
        )
    except Exception as error:
        raise ValidationError(str(error)) from error

    return config


def render_config(config_name, config):
    """Render the config dict as a python module, one entry per line for clean diffs."""
    settings = config["settings"]

    def format_value(value):
        if isinstance(value, bool):
            return "True" if value else "False"
        if isinstance(value, (int, float)):
            return repr(value)
        if isinstance(value, (set, list)):
            return "{" + ", ".join(repr(item) for item in sorted(value)) + "}"
        return repr(value)

    lines = [
        "import sqlalchemy.types as dtype",
        "",
        f"# Generated by `init.py` as `{config_name}`",
        "",
        "config_dict = {",
        '    "tables": {',
    ]
    for table_name, table_dict in config["tables"].items():
        lines.append(f'        "{table_name}": {{')
        lines.append('            "values": {')
        for src, dst in table_dict["values"].items():
            lines.append(f'                "{src}": "{dst}",')
        lines.append("            },")
        if table_dict.get("dtypes"):
            lines.append('            "dtypes": {')
            for dst, dtype_instance in table_dict["dtypes"].items():
                lines.append(f'                "{dst}": dtype.{dtype_instance!r},')
            lines.append("            },")
        lines.append("        },")
    lines.extend([
        "    },",
        '    "settings": {',
    ])
    for key, value in settings.items():
        lines.append(f'        "{key}": {format_value(value)},')
    lines.extend(["    },", "}"])
    return "\n".join(lines) + "\n"


class ColumnRow(Horizontal):
    """One IMDb column: include checkbox, SQL name input, dtype input."""

    def __init__(self, col):
        self.col = col
        children = [
            Checkbox(
                label=" ",
                value=True,
                id=f"include-{col}",
                classes="include",
                disabled=col == "tconst",
            ),
            Label(col, classes="colname"),
            Input(value=SNAKE_CASE_DEFAULTS[col], id=f"name-{col}"),
            Input(value=repr(IMDB_DATA_ALLOWED_COLUMNS[col]), id=f"dtype-{col}"),
        ]
        super().__init__(*children, id=f"row-{col}")


class PreviewScreen(ModalScreen[bool]):
    """Shows the generated config; dismisses True to write it to disk."""

    CSS = """
    PreviewScreen { align: center middle; }
    #preview-dialog { width: 90%; height: 85%; border: round $success; background: $surface; }
    #preview-code { height: 1fr; overflow: auto; }
    #preview-actions { height: 3; align: center middle; }
    """

    def __init__(self, content, exists):
        super().__init__()
        self.content = content
        self.exists = exists

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="preview-dialog"):
            with VerticalScroll(id="preview-code"):
                yield Static(Syntax(self.content, "python", theme="ansi_dark"))
            with Horizontal(id="preview-actions"):
                yield Button(
                    "Overwrite file" if self.exists else "Write file", id="write"
                )
                yield Button("Back to editing", id="back")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "write")


class ConfigWizard(App):
    TITLE = "imdb-dataset-to-sql config wizard"
    CSS = """
    #actions { height: 3; align: center middle; }
    #generate { width: 30; }
    .meta-row { height: 3; }
    .meta-label { width: 28; padding: 1 1; background: $surface; }
    .section { padding: 0 1; background: $primary; color: $text; }
    ColumnRow { height: 3; }
    ColumnRow Label { width: 16; padding: 1 1; }
    ColumnRow .include { width: 4; }
    ColumnRow Input { width: 1fr; }
    """

    def __init__(self, configs_dir=None):
        super().__init__()
        self.configs_dir = configs_dir or Path("configs")

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="form"):
            with Horizontal(classes="meta-row"):
                yield Label("Config name (CONFIG=<name>)", classes="meta-label")
                yield Input(placeholder="my_config", id="config-name")
            with Horizontal(classes="meta-row"):
                yield Label("Main table name", classes="meta-label")
                yield Input(value="title", id="title-table")
            with Horizontal(classes="meta-row"):
                yield Label("Genres table name", classes="meta-label")
                yield Input(value="title_genres", id="genres-table")

            yield Label("Columns — include, SQL name, dtype", classes="section")
            for col in ALL_COLUMNS:
                yield ColumnRow(col)

            yield Label("Settings", classes="section")
            with Horizontal(classes="meta-row"):
                yield Checkbox(label="Remove adult titles", value=True, id="remove-adult")
                yield Checkbox(label="Streaming", value=True, id="streaming")
                yield Checkbox(label="Ignore db-has-tables error", value=True, id="ignore-error")
                yield Checkbox(label="Batching", value=True, id="batching")
            with Horizontal(classes="meta-row"):
                yield Checkbox(
                    label="Split genres into ref table", value=True, id="split-genres"
                )
                yield Checkbox(
                    label="Convert titleType to int ref table",
                    value=True,
                    id="convert-titletype",
                )
            with Horizontal(classes="meta-row"):
                yield Label("Batch count", classes="meta-label")
                yield Input(value="1", id="batch-count")
            with Horizontal(classes="meta-row"):
                yield Label("Blocked titleTypes", classes="meta-label")
                yield Input(value="tvEpisode, videoGame, tvShort", id="blocked-tt")
            with Horizontal(classes="meta-row"):
                yield Label("Blocked genres", classes="meta-label")
                yield Input(value="", id="blocked-g", placeholder="Horror, Musical")

        with Horizontal(id="actions"):
            yield Button("Generate config", id="generate")

    def comma_set(self, widget_id):
        raw = self.query_one(widget_id, Input).value
        return {item.strip() for item in raw.split(",") if item.strip()}

    def collect_answers(self):
        name = self.query_one("#config-name", Input).value.strip()
        if not name:
            raise ValidationError("Config name is empty")
        try:
            validate_config_name(name, self.configs_dir)
        except FileExistsError:
            pass  # writing over an existing config is allowed; preview shows Overwrite

        columns = [
            col
            for col in ALL_COLUMNS
            if self.query_one(f"#include-{col}", Checkbox).value
        ]

        renames = {}
        dtype_overrides = {}
        errors = []
        for col in columns:
            sql_name = self.query_one(f"#name-{col}", Input).value.strip()
            if not sql_name:
                errors.append(f"`{col}`: SQL column name is empty")
            renames[col] = sql_name

            raw_dtype = self.query_one(f"#dtype-{col}", Input).value.strip()
            if not raw_dtype or raw_dtype == repr(IMDB_DATA_ALLOWED_COLUMNS[col]):
                continue
            try:
                dtype_overrides[col] = parse_dtype(raw_dtype)
            except ValidationError as error:
                errors.append(str(error))
        if errors:
            raise ValidationError("; ".join(errors))

        raw_batch_count = self.query_one("#batch-count", Input).value.strip()
        if not raw_batch_count.isdigit() or int(raw_batch_count) < 1:
            raise ValidationError("`batch_count` must be a whole number >= 1")

        answers = {
            "columns": columns,
            "title_table_name": self.query_one("#title-table", Input).value.strip(),
            "genres_table_name": self.query_one("#genres-table", Input).value.strip(),
            "is_split_genres": self.query_one("#split-genres", Checkbox).value
            and "genres" in columns,
            "is_convert_title_type": self.query_one(
                "#convert-titletype", Checkbox
            ).value
            and "titleType" in columns,
            "is_remove_adult": self.query_one("#remove-adult", Checkbox).value,
            "is_streaming": self.query_one("#streaming", Checkbox).value,
            "is_ignore_db_has_tables_error": self.query_one(
                "#ignore-error", Checkbox
            ).value,
            "is_batching": self.query_one("#batching", Checkbox).value,
            "batch_count": int(raw_batch_count),
            "renames": renames,
            "dtype_overrides": dtype_overrides,
            "blocked_titletypes": self.comma_set("#blocked-tt"),
            "blocked_genres": self.comma_set("#blocked-g"),
        }
        return name, answers

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id != "generate":
            return
        try:
            name, answers = self.collect_answers()
            config = build_config(answers)
        except ValidationError as error:
            self.notify(str(error), title="Invalid input", severity="error")
            return

        content = render_config(name, config)
        exists = (self.configs_dir / f"{name}.py").exists()

        def on_confirm(write):
            if not write:
                return
            target = self.configs_dir / f"{name}.py"
            target.write_text(content)
            self.notify(f"Wrote {target}")
            self.exit()

        self.push_screen(PreviewScreen(content, exists), on_confirm)


if __name__ == "__main__":
    ConfigWizard().run()
