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
