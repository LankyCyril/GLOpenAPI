# GLOpenAPI: NASA GeneLab Open API

__(Formerly known as GeneFab3)__

Provides cross-dataset access to multi-omics annotation and data from the
[GeneLab data repository](https://genelab-data.ndc.nasa.gov/genelab/projects);
is currently deployed at https://visualization.genelab.nasa.gov/GLOpenAPI/ and
powers the [GeneLab data visualization dashboard](https://visualization.genelab.nasa.gov/data/).

Note: if you intend to use it for downstream applications and analyses, there is
no need to install anything yourself; skip to the [Usage](#usage) section for
more information.


## Installation

It is possible, however, to deploy GLOpenAPI yourself as well.


### Requirements

* Python 3 (developed and tested on 3.6.10)
* MongoDB (developed and tested on 4.0.2-2-gddce0aa9fc)
* SQLite3 (developed and tested on 3.32.3 2020-06-18 14:00:33)
* pymongo
* flask
* flask-compress
* isatools
* numpy
* pandas
* natsort
* filelock


### How the GLOpenAPI backend operates

In general, GLOpenAPI:
* Maintains one daemon thread which continuously caches and updates metadata for
  all GeneLab datasets, and stores it in a MongoDB database
  (`db_name="genefab3"` by default):
  * investigation, samples, and assays annotation from datasets' ISA ZIP files,
  * and all publicly available file names and URLs associated with each dataset.
* Caches data on-demand (i.e., the first time it is requested by a user and then
  any time the source file is updated in the GeneLab repository) in SQLite3
  databases (which, of course, should be writable by GLOpenAPI); by default:
  * `.genefab3.sqlite3/blobs.db` for binary blobs and
  * `.genefab3.sqlite3/tables.db` for tabular data.
* Caches tabular data *representations* on-demand (for example, JSON-formatted
  merged unnormalized RNA-Seq counts, or a CSV-formatted differential analysis
  table subset by `Amean > 10`, etc) &ndash; again, the first time a specific
  query is requested, and then any time the source file(s) are updated;
  * `.genefab3.sqlite3/response-cache.db` by default.

Please refer to the comments in the `app.py` file for configuration options and
for the birds-eye view of the backend.


#### mod_wsgi primer

The installation process is typical for a Flask app, e.g.
with [mod_wsgi](https://flask.palletsprojects.com/en/2.0.x/deploying/mod_wsgi/).
Note that the name of the singleton instance of the app is called `flask_app`,
therefore in the `.wsgi` file you will need something similar to the following
line of code: `from app import flask_app as app`.


## Usage

Please refer to the description of the structure and the query syntax over at
the [API landing page](https://visualization.genelab.nasa.gov/GLOpenAPI/); in
JavaScript-capable browsers it also provides an interactive URL builder.

Basic (and very naive) examples of using the API programmatically (e.g., in
Python) are available in this repository under
[./examples](https://github.com/LankyCyril/GLOpenAPI/tree/master/examples);
for instance,
[heatmap.ipynb](https://github.com/LankyCyril/GLOpenAPI/tree/master/examples/heatmap.ipynb).
