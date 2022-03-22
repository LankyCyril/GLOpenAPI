#!/usr/bin/env python
from sys import stderr
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from random import shuffle
from itertools import combinations_with_replacement
from json import dumps
from pandas import read_csv
from urllib.parse import quote
from functools import partial


TESTS = type("Tests", (list,), dict(register=lambda s,c: s.append(c) or c))()


ARG_RULES = {
    ("api_root",): {
        "help": "API root URL", "type": str, "nargs": "?",
        "default": "http://127.0.0.1:5000/",
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
    args.tests = args.tests.strip("'\"").split(",") if args.tests else []
    return args


def get_object(args, view, query, reader):
    full_query, reader_kwargs, post = {**query}, {}, lambda _:_
    if reader == read_csv:
        reader_kwargs["low_memory"] = False
        reader_kwargs["escapechar"] = "#"
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
    url = f"{args.api_root.rstrip('/')}/{view.strip('/')}/?" + "&".join(
        quote(k) if (v == "") else f"{quote(k)}={quote(str(v))}"
        for k, v in full_query.items()
    )
    if args.verbose:
        print(f"  < URL: {url}", file=stderr)
    return post(reader(url, **reader_kwargs))


class Test():
    cross_dataset, multiple_datasets = False, False
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
        else:
            self.target_datasets = {"*"}
        for i, ds in enumerate(self.target_datasets, start=1):
            print(f"  Round {i}, dataset(s) {ds}...", file=stderr)
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
            self.results[key] = dict(status=status, error=error)
            if (status != 200):
                self.n_errors += 1
                if args.stop_on_error:
                    break


@TESTS.register
class InvestigationStudyComment(Test):
    cross_dataset = False
    multiple_datasets = False
    def seed(self):
        # TODO: remove:
        self.view, self.query = "samples", {"investigation.study": ""}
        self.metadata = get_object(self.args, self.view, self.query, read_csv)
        self.datasets = set(self.metadata.index.get_level_values(0))
    def run(self, datasets=None):
        go = partial(get_object, args=self.args, reader=read_csv)
        metadata = go(view="samples", query={"investigation.study": ""})
        c1, c2 = "investigation.study", "comment.mission start"
        if (c1, c2) not in metadata.columns:
            return -1, f"'{c1}.{c2}' missing"
        return 200


def main(args):
    if args.list_tests:
        print(f"{'#name':32} {'#multiple_datasets':20} #cross_dataset")
        for t in TESTS:
            _stmd = str(t.multiple_datasets)
            print(f"{t.__name__:32} {_stmd:20} {t.cross_dataset}")
    else:
        results = {}
        for Test in TESTS:
            if (not args.tests) or (Test.__name__ in args.tests):
                print(f"Running {Test.__name__}...", file=stderr)
                test = Test(args)
                results[Test.__name__] = test.results
                if test.n_errors and args.stop_on_error:
                    break
        print(dumps(results, sort_keys=True, indent=4))
    return 0


if __name__ == "__main__":
    ap_kwargs = dict(width=107, max_help_position=36)
    ApFormatter = lambda prog: ArgumentDefaultsHelpFormatter(prog, **ap_kwargs)
    parser = ArgumentParser(formatter_class=ApFormatter)
    for argnames, argopts in ARG_RULES.items():
        parser.add_argument(*argnames, **argopts)
    args = preprocess_args(parser.parse_args())
    returncode = main(args)
    exit(returncode)
