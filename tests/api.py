#!/usr/bin/env python
from sys import stderr
from os import get_terminal_size
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from random import shuffle
from itertools import combinations_with_replacement, takewhile
from itertools import combinations, permutations
from json import dumps, loads
from pandas import read_csv
from urllib.parse import quote
from functools import partial
from contextlib import contextmanager
from requests import get as rget


TERMWIDTH = get_terminal_size().columns
TESTS = type("Tests", (list,), dict(register=lambda s,c: s.append(c) or c))()

ARG_RULES = {
    ("api_root",): {
        "help": "API root URL", "type": str, "nargs": "?",
        "default": "http://127.0.0.1:5000",
    },
    ("--target-api-version", "--tv"): {
        "help": "Target API version", "default": "4.0.0",
        "metavar": "V",
    },
    ("--list-tests", "--lt"): {
        "help": "List available test names", "action": "store_true",
    },
    ("--stop-on-error", "--soe"): {
        "help": "Stop after first error", "action": "store_true",
    },
    ("--tests", "-t"): {
        "help": "Restrict to specific test names (comma-separated)",
        "metavar": "T",
    },
    ("--n-datasets", "-n"): {
        "help": "Max number of datasets to run tests on (randomly selected)",
        "metavar": "N", "type": int, "default": 12,
    },
    ("--favorites", "-f"): {
        "help": "Always non-randomly include datasets (comma-separated)",
        "metavar": "F", "default": "'1,4,30,42'",
    },
    ("--max-combinations", "-M"): {
        "help": "Maximum number of cross-dataset runs per test",
        "metavar": "M", "type": int, "default": 4,
    },
    ("--max-combined", "-m"): {
        "help": "Maximum number of datasets per cross-dataset test run",
        "metavar": "m", "type": int, "default": 2,
    },
    ("--verbose", "-v"): {
        "help": "Print additional information to stderr",
        "action": "store_true",
    },
    ("--ansi-color", "-c"): {
        "help": "Color results in progress with ANSI colors",
        "action": "store_true",
    },
}


def preprocess_args(args):
    try:
        args.target_api_version = tuple(map(
            int, args.target_api_version.split("."),
        ))
    except ValueError:
        msg = f"Bad version string: {args.target_api_version}"
        exit(print(msg, file=stderr) or 1)
    args.favorites = {
        f if f.startswith("GLDS-") else f"GLDS-{f}"
        for f in args.favorites.strip("'\"").split(",")
    }
    args.tests = set(args.tests.strip("'\"").split(",")) if args.tests else []
    return args


def colorify(results):
    if results.get("error"):
        return f"\x1b[91m{results!r}\x1b[0m"
    elif results.get("warnings"):
        return f"\x1b[93m{results!r}\x1b[0m"
    else:
        return f"\x1b[92m{results!r}\x1b[0m"


class Test():
    multiple_datasets = False
    cross_dataset = False
    target_datasets = "*"
    min_api_version = (0, 0, 0)
 
    def __init__(self, args):
        self.args, self.results, self.n_errors, self.n_warnings = args, {}, 0, 0
        self.seed()
        if self.multiple_datasets:
            self.target_datasets = list(self.datasets & args.favorites)
            self.target_datasets = self.target_datasets[:args.n_datasets]
            non_favorite_datasets = list(self.datasets - args.favorites)
            shuffle(non_favorite_datasets)
            for dataset in non_favorite_datasets:
                if len(self.target_datasets) < args.n_datasets:
                    self.target_datasets.append(dataset)
            if self.cross_dataset:
                cwr = list(map(set, combinations_with_replacement(
                    self.target_datasets, args.max_combined,
                )))
                shuffle(cwr)
                self.target_datasets = cwr[:args.max_combinations]
        for i, ds in enumerate(self.target_datasets, start=1):
            print(f"  Round {i} STARTED: using dataset(s) {ds}", file=stderr)
            try:
                status, error, *warnings = self.run(ds, args=args)
            except Exception as e:
                status, error, warnings = -1, repr(e), []
            key = ",".join(sorted(ds)) if isinstance(ds, set) else ds
            _results = dict(
                success=(status==200), status=status,
                warnings=warnings, error=error,
            )
            self.results[key] = _results
            if args.ansi_color:
                print(f"  Round {i} RESULTS: {colorify(_results)}", file=stderr)
            else:
                print(f"  Round {i} RESULTS: {_results!r}", file=stderr)
            if status != 200:
                self.n_errors += 1
                if args.stop_on_error:
                    break
            self.n_warnings += len(warnings)
 
    def seed(self):
        pass
 
    @contextmanager
    def go(self, view="samples", query=(), reader=read_csv):
        if isinstance(query, dict):
            full_query = list(query.items())
        elif isinstance(query, set):
            full_query = [(k, "") for k in query]
        else:
            full_query = query[:]
        reader_kwargs, post = {}, lambda _:_
        if reader == read_csv:
            reader_kwargs.update(dict(low_memory=False, escapechar="#"))
            if view == "data":
                reader_kwargs["header"] = [0, 1, 2]
                post = lambda df: df.set_index(df.columns[0])
            else:
                reader_kwargs["header"] = [0, 1]
                post = lambda df: df.set_index([
                    c for c in df.columns.tolist() if c[0] == "id"
                ])
            for k, v in full_query:
                if (k == "format") and (v == "tsv"):
                    reader_kwargs["sep"] = "\t"
                    break
        def _kv2str(k, v):
            head = quote(k) if isinstance(k, str) else quote(".".join(k))
            return head if (v == "") else f"{head}={quote(str(v))}"
        url = f"{self.args.api_root}/{view}/?" + "&".join(
            _kv2str(k, v) for k, v in full_query
        )
        if self.args.verbose:
            print(f"  < URL: {url}", file=stderr)
        try:
            yield post(reader(url, **reader_kwargs))
        except Exception as e:
            raise self.generate_error_description(url, e)
 
    def generate_error_description(self, url, e_orig):
        try:
            with rget(url) as response:
                ej = loads("\n".join(
                    takewhile(lambda s: s != "", response.text.split("\n"))
                ))
        except Exception as e_current:
            return IOError(f"{str(e_orig)} [c.o. {e_current!r}] [URL: {url}]")
        else:
            return IOError("".join((
                    ej.get('exception_type', 'Exception'),
                    f"({ej.get('exception_value', '')!r})",
                    f" [c.o. {e_orig!r}] [URL: {url}]",
            )))


@TESTS.register
class InvestigationStudyComment(Test):
    multiple_datasets = False
    cross_dataset = False
    min_api_version = (3, 1, 0)
 
    def run(self, datasets=None, *, args=None):
        t0, b = "investigation.study", [
            "comment.mission start", "comment.mission end",
            "comment.space program", "study title",
        ]
        with self.go(query={t0}) as data:
            if (t0, b[0]) not in data.columns:
                return -1, f"'{t0}.{b[0]}' missing"
            if (t0, "study title.") in data.columns:
                return -1, f"extra dot in '{t0}.study title.'"
        with self.go(query={(t0, b[0]), (t0, b[1])}) as data:
            if (data.shape[0] == 0) or (data.shape[1] < 2):
                return -1, f"direct query for '{t0}.{b[0]}/{t0}.{b[1]}' failed"
        with self.go(query={(t0, b[2])}) as data:
            if (data.shape[1] > 1):
                return -1, "querying for one 'comment' field retrieves many"
        with self.go(query={(t0, b[3])}) as data:
            if any(c.startswith("comment") for _, c in set(data.columns)):
                return -1, "querying for non-comment also retrieves comments"
        return 200, None


@TESTS.register
class LargeMetadata(Test):
    multiple_datasets = False
    cross_dataset = False
    min_api_version = (3, 1, 0)
 
    def run(self, datasets=None, *, args=None):
        t0, b = "study", ["factor value", "parameter value", "characteristics"]
        with self.go(query={(t0, b[0])}):
            pass
        with self.go(query={(t0, b[0]), (t0, b[1]), (t0, b[2])}) as data:
            if (data.shape[0] == 0) or (data.shape[1] == 0):
                return -1, "retrieving all 'study.*' returns nothing"
        return 200, None


@TESTS.register
class MetadataCombinationsAnd(Test):
    multiple_datasets = False
    cross_dataset = False
    min_api_version = (3, 1, 0)
 
    def run(self, datasets=None, *, args=None):
        with self.go("assays", query={"study.factor value"}) as assays:
            n_factors = assays.sum(axis=1)
            good_assays = assays[(n_factors>1) & (n_factors<=args.max_combined)]
            potential_targets = [
                [ids2, row[row==True]] for ids2, row in good_assays.iterrows()
            ]
            shuffle(potential_targets)
            targets = potential_targets[:args.max_combinations]
        if len(targets) == 0:
            return -1, "no assays satisfying -m and -M criteria"
        else:
            for ids2, row in targets:
                if args.target_api_version < (4,):
                    query = {f"{k}.{v}" for k, v in row.index}
                else:
                    query = {f"={k}.{v}" for k, v in row.index}
                with self.go("samples", query=query) as metadata:
                    for ids3 in metadata.index.tolist():
                        if ids2 == ids3[:2]:
                            break
                    else:
                        msg = f"for {query}, expected {ids2} but not retrieved"
                        return -1, msg
            return 200, None


@TESTS.register
class DataColumns(Test):
    multiple_datasets = True
    cross_dataset = False
    min_api_version = (3, 1, 0)
 
    def seed(self):
        with self.go(query={"file.datatype": "visualization table"}) as data:
            self.datasets = set(data.index.get_level_values(0))
 
    def run(self, datasets, *, args=None):
        q = {"file.datatype": "visualization table", "id": datasets}
        with self.go("samples", query=q) as metadata:
            assays = list(set(metadata.index.get_level_values(1)))
            if len(assays) > 1:
                shuffle(assays)
                q["id"] = q["id"] + "/" + assays[0]
        with self.go("data", query={**q, "schema": 1}) as schema:
            if schema.index.names[0][:2] != ("*", "*"):
                return -1, "data index top level is not '*', '*'"
            index_col = schema.index.names[0][2]
            columns = list(schema.columns.get_level_values(2))
            shuffle(columns)
            column_queries = {f"c.{c}": "" for c in [index_col, *columns[:3]]}
        desc = "retrieving just the data index"
        try:
            with self.go("data", query={**q, f"c.{index_col}": ""}) as data:
                pass
        except Exception as e:
            return -1, f"{desc} fails w/ {e}"
        else:
            if data.shape[1] != 0:
                return -1, f"{desc} produces the wrong # of columns"
        try:
            with self.go("data", query={**q, **column_queries}) as data:
                pass
        except Exception as e:
            return -1, f"retrieving data columns fails w/ {e}"
        else:
            if (data.shape[1] != 3) or (data.index.names[0][2] != index_col):
                return -1, "index column lost when retrieving other data cols"
        return 200, None


@TESTS.register
class MetadataToVizColumn(Test):
    multiple_datasets = True
    cross_dataset = False
    min_api_version = (3, 1, 0)
 
    GROUP_PREFIXES = ["Group.Mean", "Group.Stdev"]
    CONTRAST_PREFIXES = [
        "Log2fc", "Updown_Log2fc", "P.value", "Adj.p.value", "Sig.05", "Sig.1",
        "Log2_P.value", "Log2_Adj.p.value",
    ]
    n_cols = 3
 
    def seed(self):
        with self.go(query={"file.datatype": "visualization table"}) as data:
            self.datasets = set(data.index.get_level_values(0))
 
    def run(self, datasets, *, args=None):
        data_q = {"file.datatype": "visualization table", "id": datasets}
        meta_q = {"study.factor value": "", "id": datasets}
        shuffle(self.GROUP_PREFIXES)
        shuffle(self.CONTRAST_PREFIXES)
        potential_data_columns = set()
        with self.go("samples", query=meta_q) as metadata:
            factor_permutations = {
                " & ".join(map(str, p))
                for _, r in metadata.drop_duplicates().iterrows()
                for p in permutations(r, len(r))
            }
            for prefix in self.GROUP_PREFIXES[:3]:
                for fp in factor_permutations:
                    potential_data_columns.add(f"{prefix}_{fp}")
            factor_combinations = {
                f"({a})v({b})" for a, b in combinations(factor_permutations, 2)
            }
            for prefix in self.CONTRAST_PREFIXES[:3]:
                for fc in factor_combinations:
                    potential_data_columns.add(f"{prefix}_{fc}")
        with self.go("data", query={**data_q, "schema": 1}) as schema:
            data_columns = list(
                potential_data_columns & set(schema.columns.get_level_values(2))
            )
            if len(data_columns) == 0:
                return 200, False, (
                    "No mapping from metadata to data columns; "
                    "possibly pending factor value replacements"
                )
            shuffle(data_columns)
            column_queries = {f"c.{c}": "" for c in data_columns[:self.n_cols]}
        with self.go("data", query={**data_q, **column_queries}) as data:
            if (data.shape[0] == 0) or (data.shape[1] != self.n_cols):
                e = "; ".join(f"{c!r}" for c in column_queries)
                return -1, f"metadata values and data columns do not match: {e}"
            return 200, None


def main(args):
    if args.list_tests:
        print(
            f"{'#name':32}", f"{'#multiple_datasets':19}",
            f"{'#min_api_version':17}", "#cross_dataset",
        )
        for Test in TESTS:
            print(
                f"{Test.__name__:32}", f"{str(Test.multiple_datasets):19}",
                f"{'.'.join(map(str, Test.min_api_version)):17}",
                f"{Test.cross_dataset}",
            )
        return 0
    else:
        results = {"n_errors": 0, "n_warnings": 0, "success": None, "tests": {}}
        for Test in TESTS:
            if (not args.tests) or (Test.__name__ in args.tests):
                if Test.min_api_version <= args.target_api_version:
                    print(f"Running {Test.__name__}...", file=stderr)
                    test = Test(args)
                    results["tests"][Test.__name__] = test.results
                    if test.n_errors and args.stop_on_error:
                        break
                    results["n_errors"] += test.n_errors
                    results["n_warnings"] += test.n_warnings
                else:
                    msg = f"Skipping {Test.__name__} (targets newer version)"
                    print(msg, file=stderr)
        results["success"] = (results["n_errors"] == 0)
        print(dumps(results, sort_keys=True, indent=4))
        return results["n_errors"]


if __name__ == "__main__":
    parser = ArgumentParser(formatter_class=partial(
        ArgumentDefaultsHelpFormatter, width=TERMWIDTH, max_help_position=36,
    ))
    [parser.add_argument(*a, **o) for a, o in ARG_RULES.items()]
    exit(main(preprocess_args(parser.parse_args())))
