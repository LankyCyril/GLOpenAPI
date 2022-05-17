Version 4.0.0-alpha2 (2022-05-17)

- Features (internal):
    - Script for testing the API: tests/api.py
- Fixes:
    - Fix double-escaping of /data/ column names that lead to lookup errors


Version 4.0.0-alpha1 (2022-03-28)

- Hotfix: resolve data file names correctly (fixes wrong return in one of the
  internal functions, see commit 72a5996)


Version 4.0.0-alpha0 (2022-01-27)

- Rename backend to GLOpenAPI
- Features that MAY BE CONSIDERED BREAKING CHANGES:
    - Metadata:
        - If no metadata constraints are given in the query, return all fields (in /metadata/, /samples/, and /assays/);
          this is now consistent with the behavior of the /data/ endpoint (which has always returned all columns if no
          constraints were given)
        - When querying metadata without providing a value (e.g., "&study.factor value.spaceflight&..."), include this
          field in the output even if it contains NAs (in other words, show the column, but do not constrain by its
          contents)
        - To constrain to non-NA values, use syntax "&=field"
          (with a leading equals sign, e.g. "&=study.factor value.spaceflight&...")
        - NOTE: the behavior of direct querying for a value (e.g. "&study.factor value.spaceflight=Ground Control")
          is unaffected by these changes
        - NOTE: the behavior of querying without providing a value (e.g., "&study.factor value.spaceflight&...")
          should be unaffected when querying within single assays -- simply due to the fact that within an assay, all
          fields are simultaneously either defined or not defined. However, take note that theoretically, any queries
          without an equals sign may return fields with NAs.
    - Data:
        - Querying for a column value (e.g. "&column.Log2FC>2") constrains the output to the columns queried for.
          The remaining columns can be included back by using the wildcard "&column.*"
          (or its alias, "&c.*" -- see below)
- Features:
    - Metadata:
        - Provide endpoint /metadata/, which is an alias to /samples/
        - Provide endpoint /metadata-counts/, which returns a JSON of value counts for each queried nested field:
            - each metadata value is represented by an object with three fields: "accessions", "assays", "samples" --
              each of these fields contains the number of respective entires that nave this value;
            - only the JSON format is valid (and is the default, so one may omit the "&format=json" argument entirely)
    - Data:
        - Provide a wildcard argument "&column.*" (and its alias, "&c.*") to force inclusion of all /data/ columns even
          when a query would constrain the output to a column
- Various:
    - In debug mode, make it possible to launch the app with caches disabled (`./debug development nocache`)
      (this is only possible on staging/development servers, not on the production server)


Version 3.1.0 (2021-12-13)

- Fixes:
    - Sort metadata using multiple indices, preventing high RAM usage (and therefore errors on exceeding RAM limits)
      when large tables are requested
    - Allow to request data table index by its name as if it were a column (e.g., `&column.index`, `&column.TAIR`, etc)
- Features:
    - Allow to query comments (logged under "investigation.study.comment" in ISA-Tab), e.g.:
        - `&investigation.study.comment`
        - `&investigation.study.comment.mission start&investigation.study.comment.mission end`
        - `&investigation.study.comment.space program=NASA|JAXA`
    - Add changelog at /changelog/
- Various:
    - Keep landing page cached between metadata update cycles


Version 3.0.2 (2021-09-13)

- Fixes:
    - "Material type" metadata values are always accessible through the "study.characteristics.material type" query,
      even in cases where the dataset's ISA-Tab has it logged in the legacy location ("study.material type")


Version 3.0.1 (2021-08-12)

- Features:
    - Introduced semantic versioning
    - Version number available via /version/ endpoint
- Improvements:
    - Provided example Jupyter notebook: examples/heatmap.ipynb
    - Expanded the readme
- Various:
    - In DEBUG mode, log all URLs that are accessed internally
    - Do not prefill example dropdowns on landing page
