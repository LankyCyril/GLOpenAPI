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
