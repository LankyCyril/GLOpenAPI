#!/usr/bin/env python
from sys import stderr
from os import get_terminal_size
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from random import shuffle
from itertools import combinations_with_replacement
from json import dumps
from pandas import read_csv
from urllib.parse import quote
from functools import partial
from contextlib import contextmanager


TERMWIDTH = get_terminal_size().columns
TESTS = type("Tests", (list,), dict(register=lambda s,c: s.append(c) or c))()

ARG_RULES = {
    ("api_root",): {
        "help": "API root URL", "type": str, "nargs": "?",
        "default": "http://127.0.0.1:5000",
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
}


def preprocess_args(args):
    args.favorites = {
        f if f.startswith("GLDS-") else f"GLDS-{f}"
        for f in args.favorites.strip("'\"").split(",")
    }
    args.tests = set(args.tests.strip("'\"").split(",")) if args.tests else []
    return args


class Test():
    cross_dataset = False
    multiple_datasets = False
    target_datasets = "*"
 
    def __init__(self, args):
        self.args, self.results, self.n_errors = args, {}, 0
        self.seed()
        if self.multiple_datasets:
            self.target_datasets = self.datasets & args.favorites
            non_favorite_datasets = list(self.datasets - args.favorites)
            shuffle(non_favorite_datasets)
            for dataset in non_favorite_datasets:
                if len(self.target_datasets) < args.n_datasets:
                    self.target_datasets.add(dataset)
            if self.cross_dataset:
                cwr = list(map(set, combinations_with_replacement(
                    self.target_datasets, args.max_combined,
                )))
                shuffle(cwr)
                self.target_datasets = cwr[:args.max_combinations]
        for i, ds in enumerate(self.target_datasets, start=1):
            print(f"  Round {i} STARTED: using dataset(s) {ds}", file=stderr)
            try:
                ret = self.run(ds)
            except Exception as e:
                status, error = -1, repr(e)
            else:
                if isinstance(ret, (list, tuple)):
                    status, error = [*ret[:2], 200, False][:2]
                else:
                    status, error = ret, False
            key = ",".join(sorted(ds)) if isinstance(ds, set) else ds
            _results = dict(status=status, error=error, success=(status==200))
            self.results[key] = _results
            print(f"  Round {i} RESULTS: {_results}", file=stderr)
            if status != 200:
                self.n_errors += 1
                if args.stop_on_error:
                    break
 
    def seed(self):
        pass
 
    @contextmanager
    def go(self, view, query, reader=read_csv):
        full_query, reader_kwargs, post = {**query}, {}, lambda _:_
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
            if query.get("format") == "tsv":
                reader_kwargs["sep"] = "\t"
        def _kv2str(k, v):
            head = quote(k) if isinstance(k, str) else quote(".".join(k))
            return head if (v == "") else f"{head}={quote(str(v))}"
        url = f"{self.args.api_root}/{view}/?" + "&".join(
            _kv2str(k, v) for k, v in full_query.items()
        )
        if self.args.verbose:
            print(f"  < URL: {url}", file=stderr)
        yield post(reader(url, **reader_kwargs))


@TESTS.register
class InvestigationStudyComment(Test):
    cross_dataset = False
    multiple_datasets = False
 
    def run(self, datasets=None):
        t0, b = "investigation.study", [
            "comment.mission start", "comment.mission end",
            "comment.space program", "study title",
        ]
        with self.go("samples", {t0: ""}) as metadata:
            if (t0, b[0]) not in metadata.columns:
                return -1, f"'{t0}.{b[0]}' missing"
            if (t0, "study title.") in metadata.columns:
                return -1, f"extra dot in '{t0}.study title.'"
        with self.go("samples", {(t0, b[0]): "", (t0, b[1]): ""}) as metadata:
            if (metadata.shape[0] == 0) or (metadata.shape[1] < 2):
                return -1, f"direct query for '{t0}.{b[0]}/{t0}.{b[1]}' failed"
        with self.go("samples", {(t0, b[2]): ""}) as metadata:
            if (metadata.shape[1] > 1):
                return -1, "querying for one 'comment' field retrieves many"
        with self.go("samples", {(t0, b[3]): ""}) as metadata:
            if any(c.startswith("comment") for _, c in set(metadata.columns)):
                return -1, "querying for non-comment also retrieves comments"
        return 200


def main(args):
    if args.list_tests:
        print(f"{'#name':32} {'#multiple_datasets':20} #cross_dataset")
        for t in TESTS:
            _stmd = str(t.multiple_datasets)
            print(f"{t.__name__:32} {_stmd:20} {t.cross_dataset}")
        return 0
    else:
        results, n_errors = {}, 0
        for Test in TESTS:
            if (not args.tests) or (Test.__name__ in args.tests):
                print(f"Running {Test.__name__}...", file=stderr)
                test = Test(args)
                results[Test.__name__] = test.results
                if test.n_errors and args.stop_on_error:
                    break
                n_errors += test.n_errors
        print(dumps(results, sort_keys=True, indent=4))
        return n_errors


if __name__ == "__main__":
    parser = ArgumentParser(formatter_class=partial(
        ArgumentDefaultsHelpFormatter, width=TERMWIDTH, max_help_position=36,
    ))
    [parser.add_argument(*a, **o) for a, o in ARG_RULES.items()]
    exit(main(preprocess_args(parser.parse_args())))
