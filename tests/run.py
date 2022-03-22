#!/usr/bin/env python
from sys import stderr
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter


TESTS = type("Tests", (list,), dict(register=lambda s,c: s.append(c) or c))()


ARG_RULES = {
    ("api_root",): {
        "help": "API root URL", "type": str,
        "default": "http://127.0.0.1:5000/",
    },
    ("--list-tests", "--lt"): {
        "help": "List available test names", "action": "store_true",
    },
    ("--tests", "-t"): {
        "help": "Restrict to specific test names (comma-separated)",
        "metavar": "T",
    },
    ("--n-datasets", "-n"): {
        "help": "Number of datasets to run tests on (randomly selected)",
        "metavar": "N", "type": int, "default": 12,
    },
    ("--favorites", "-f"): {
        "help": "Always non-randomly include datasets (comma-separated)",
        "metavar": "F", "default": "'1,4,30,42'",
    },
    ("--max-combinations", "-M"): {
        "help": "Maximum number of cross-dataset tests to run",
        "metavar": "M", "type": int, "default": 0,
    },
    ("--max-combined", "-m"): {
        "help": "Maximum number of datasets per cross-dataset test",
        "metavar": "m", "type": int, "default": 1,
    },
}


def preprocess_args(args):
    args.favorites = [
        f if f.startswith("GLDS-") else f"GLDS-{f}"
        for f in args.favorites.strip("'\"").split(",")
    ]
    args.tests = args.tests.strip("'\"").split(",") if args.tests else []
    return args


class Test():
    multi = False
    def __init__(self, datasets):
        self.datasets = datasets


@TESTS.register
class ABC(Test):
    def run(self):
        pass


def main(args):
    if args.list_tests:
        print(f"{'name':20} multi")
        for test in TESTS:
            print(f"{test.__name__:20} {test.multi}")
    else:
        for test in TESTS:
            if (not args.tests) or (test.__name__ in args.tests):
                print(f"Running {test.__name__}...", file=stderr)
                test().run()
    return 0


if __name__ == "__main__":
    ap_kwargs = dict(width=120, max_help_position=36)
    ApFormatter = lambda prog: ArgumentDefaultsHelpFormatter(prog, **ap_kwargs)
    parser = ArgumentParser(formatter_class=ApFormatter)
    for argnames, argopts in ARG_RULES.items():
        parser.add_argument(*argnames, **argopts)
    args = preprocess_args(parser.parse_args())
    returncode = main(args)
    exit(returncode)
