from genefab3.common.exceptions import GeneFabISAException
from re import search, sub
from collections import defaultdict
from genefab3.common.exceptions import GeneFabConfigurationException
from numpy import nan
from genefab3.db.mongo.status import update_status
from types import SimpleNamespace
from zipfile import ZipFile
from io import BytesIO, StringIO
from os import path
from logging import getLogger, CRITICAL
from isatools.isatab import load_investigation
from pandas import read_csv
from pandas.errors import ParserError


class Investigation(dict):
    """Stores GLDS ISA Tab 'investigation' in accessible formats"""
 
    def __init__(self, raw_investigation, status_kwargs):
        """Convert dataframes to JSONs"""
        for real_name, isatools_name, target, pattern in self._key_dispatcher:
            if isatools_name in raw_investigation:
                content = raw_investigation[isatools_name]
                if isinstance(content, list):
                    json = [
                        self._jsonify(df, coerce_comments=True)
                        for df in content
                    ]
                else:
                    json = self._jsonify(content, coerce_comments=True)
                if isinstance(json, list):
                    if (len(json) == 1) and isinstance(json[0], list):
                        json = json[0]
                if isinstance(target, int) and isinstance(pattern, int):
                    try:
                        if len(json) != pattern:
                            raise IndexError
                        else:
                            super().__setitem__(real_name, json[target])
                    except (TypeError, IndexError, KeyError):
                        msg = "Unexpected structure of field"
                        _kw = dict(field=real_name, **status_kwargs)
                        raise GeneFabISAException(msg, **_kw)
                elif target and pattern:
                    try:
                        super().__setitem__(real_name, {
                            search(pattern, entry[target]).group(1): entry
                            for entry in json
                        })
                    except (TypeError, AttributeError, IndexError, KeyError):
                        msg = "Could not break up field by name"
                        _kw = dict(field=real_name, **status_kwargs)
                        raise GeneFabISAException(msg, **_kw)
                else:
                    super().__setitem__(real_name, json)

    _key_dispatcher = [
      # ("Real Name In Mixed Case", "as_in_isatools", target_for_keys, pattern)
        ("Ontology Source Reference", "ontology_sources", None, None),
        ("Investigation", "investigation", 0, 1),
        ("Investigation Publications", "i_publications", None, None),
        ("Investigation Contacts", "i_contacts", None, None),
        ("Study", "studies", "Study File Name", r'^s_(.+)\.txt$'),
        ("Study Design Descriptors", "s_design_descriptors", None, None),
        ("Study Publications", "s_publications", None, None),
        ("Study Factors", "s_factors", None, None),
        ("Study Assays", "s_assays", "Study Assay File Name", r'^a_(.+)\.txt$'),
        ("Study Protocols", "s_protocols", None, None),
        ("Study Contacts", "s_contacts", None, None),
    ]
 
    def _jsonify(self, df, coerce_comments=True):
        """Convert individual dataframe to JSON"""
        nn = range(0, df.shape[1])
        json = df.drop(columns=nn, errors="ignore").to_dict(orient="records")
        for entry in json:
            for key in set(entry.keys()):
                match = search(r'^Comment\s*\[\s*(.+[^\s]\s*)\]\s*$', key)
                if match:
                    field, value = match.group(1), entry[key]
                    del entry[key]
                    if "Comment" not in entry:
                        entry["Comment"] = {}
                    if coerce_comments:
                        field = field.capitalize()
                        if field not in entry["Comment"]:
                            entry["Comment"][field] = []
                        entry["Comment"][field].append(value)
                    else:
                        entry["Comment"][field] = value
            if coerce_comments and ("Comment" in entry):
                for field in set(entry["Comment"].keys()):
                    value_set = set(entry["Comment"][field])
                    if len(value_set) == 1:
                        entry["Comment"][field] = value_set.pop()
        if json:
            return json
        else:
            return [{}]


class StudyEntries(list):
    """Stores GLDS ISA Tab 'studies' records as a nested JSON"""
    _self_identifier = "Study"
 
    def __init__(self, raw_tabs, status_kwargs):
        """Convert tables to nested JSONs"""
        if self._self_identifier == "Study":
            self._by_sample_name = {}
        else: # lookup in classes like AssayEntries would be ambiguous
            self._by_sample_name = defaultdict(self._abort_lookup)
        for name, raw_tab in raw_tabs.items():
            for _, row in raw_tab.iterrows():
                if "Sample Name" not in row:
                    msg = f"{self._self_identifier} entry missing 'Sample Name'"
                    raise GeneFabISAException(msg, **status_kwargs)
                else:
                    sample_name = row["Sample Name"]
                json = self._row_to_json(
                    row, name, {**status_kwargs, "sample_name": sample_name},
                )
                super().append(json)
                if self._self_identifier == "Study":
                    if sample_name in self._by_sample_name:
                        msg = "Duplicate 'Sample Name' in Study tab"
                        _kw = dict(sample_name=sample_name, **status_kwargs)
                        raise GeneFabISAException(msg, **_kw)
                    else:
                        self._by_sample_name[sample_name] = json
 
    def _abort_lookup(self):
        """Prevents ambiguous lookup through `self._by_sample_name` in inherited classes"""
        msg = "Unique lookup by sample name not allowed for type"
        raise GeneFabConfigurationException(msg, type=type(self).__name__)
 
    def _row_to_json(self, row, name, status_kwargs):
        """Convert single row of table to nested JSON"""
        json = {"Id": {f"{self._self_identifier} Name": name}}
        protocol_ref, qualifiable = nan, None
        for column, value in row.items():
            field, subfield, extra = self._parse_field(column)
            if field == "Protocol REF":
                protocol_ref = value
            elif self._is_not_qualifier(field): # top-level field
                if not subfield: # e.g. "Source Name"
                    qualifiable = self._INPLACE_add_toplevel_field(
                        json, field, value, protocol_ref,
                    )
                else: # e.g. "Characteristics[Age]"
                    qualifiable = self._INPLACE_add_metadatalike(
                        json, field, subfield, value, protocol_ref,
                        status_kwargs,
                    )
            else: # qualify entry at pointer with second-level field
                if qualifiable is None:
                    msg = "Qualifier before main field"
                    _kw = {field: value, **status_kwargs}
                    raise GeneFabISAException(msg, **_kw)
                else:
                    self._INPLACE_qualify(
                        qualifiable, field, subfield, value,
                        status_kwargs={**status_kwargs, "name": name},
                    )
        return json
 
    def _parse_field(self, column):
        """Interpret field like 'Source Name' or 'Characteristics[sex,http://purl.obolibrary.org/obo/PATO_0000047,EFO]"""
        matcher = search(r'(.+[^\s])\s*\[\s*(.+[^\s])\s*\]\s*$', column)
        if matcher:
            field = matcher.group(1)
            subfield, *extra = matcher.group(2).split(",")
            return field, subfield, extra
        else:
            return column, None, []
 
    def _is_not_qualifier(self, field):
        """Check if `column` is none of 'Term Accession Number', 'Unit', 'Comment', any '.* REF'"""
        return (
            (field not in {"Term Accession Number", "Unit", "Comment"}) and
            (not field.endswith(" REF"))
        )
 
    def _INPLACE_add_toplevel_field(self, json, field, value, protocol_ref):
        """Add top-level key-value to json ('Source Name', 'Material Type',...), qualify with 'Protocol REF', point to resulting field"""
        value_with_protocol_ref = {"": value, "Protocol REF": protocol_ref}
        if field in json:
            json[field].append(value_with_protocol_ref)
        else: # make {"Source Name": [{"": "ABC"}]}
            json[field] = [value_with_protocol_ref]
        qualifiable = json[field][-1]
        return qualifiable
 
    def _INPLACE_add_metadatalike(self, json, field, subfield, value, protocol_ref, status_kwargs):
        """Add metadatalike to json (e.g. 'Characteristics' -> 'Age'), qualify with 'Protocol REF', point to resulting field"""
        if field not in json:
            json[field] = {}
        if subfield in json[field]:
            msg = "Duplicate field[subfield]"
            _kw = dict(field=field, subfield=subfield, **status_kwargs)
            raise GeneFabISAException(msg, **_kw)
        else: # make {"Characteristics": {"Age": {"": "36"}}}
            json[field][subfield] = {"": value}
            qualifiable = json[field][subfield]
            if field == "Parameter Value":
                qualifiable["Protocol REF"] = protocol_ref
            return qualifiable
 
    def _INPLACE_qualify(self, qualifiable, field, subfield, value, status_kwargs):
        """Add qualifier to field at pointer (qualifiable)"""
        if field == "Comment": # make {"Comment": {"mood": "cheerful"}} etc
            if "Comment" not in qualifiable:
                qualifiable["Comment"] = {"": nan}
            qualifiable["Comment"][subfield or ""] = value
        else: # make {"Unit": "percent"} etc
            if subfield:
                update_status(
                    **status_kwargs, status="warning",
                    warning="Extra info past qualifier",
                    qualifier=field, tab=self._self_identifier,
                )
            qualifiable[field] = value


class AssayEntries(StudyEntries):
    """Stores GLDS ISA Tab 'assays' records as a nested JSON"""
    _self_identifier = "Assay"


class IsaFromZip():
    """Stores GLDS ISA information retrieved from ISA ZIP file stream"""
 
    def __init__(self, data, status_kwargs=None):
        """Unpack ZIP from URL and delegate to sub-parsers"""
        _status_kws = status_kwargs or {}
        self.raw = self._ingest_raw_isa(data, _status_kws)
        self.investigation = Investigation(self.raw.investigation, _status_kws)
        self.studies = StudyEntries(self.raw.studies, _status_kws)
        self.assays = AssayEntries(self.raw.assays, _status_kws)
 
    def _ingest_raw_isa(self, data, status_kwargs):
        """Unpack ZIP from URL and delegate to top-level parsers"""
        raw = SimpleNamespace(investigation=None, studies={}, assays={})
        with ZipFile(BytesIO(data)) as archive:
            for filepath in archive.namelist():
                _, filename = path.split(filepath)
                matcher = search(r'^([isa])_(.+)\.txt$', filename)
                if matcher:
                    kind, name = matcher.groups()
                    with archive.open(filepath) as handle:
                        if kind == "i":
                            reader = self._read_investigation
                            raw.investigation = reader(handle)
                        elif kind == "s":
                            reader = self._read_tab
                            raw.studies[name] = reader(handle, status_kwargs)
                        elif kind == "a":
                            reader = self._read_tab
                            raw.assays[name] = reader(handle, status_kwargs)
        for tab, value in raw.__dict__.items():
            if not value:
                msg = "Missing ISA tab"
                raise GeneFabISAException(msg, tab=tab, **status_kwargs)
        return raw
 
    def _read_investigation(self, handle):
        """Load investigation tab with isatools, safeguard input for engine='python', suppress isatools' logger"""
        safe_handle = StringIO(
            sub( # make number of double quotes inside fields even:
                r'([^\n\t])\"([^\n\t])', r'\1""\2',
                handle.read().decode(errors="replace")
            ),
        )
        getLogger("isatools").setLevel(CRITICAL+1)
        return load_investigation(safe_handle)
 
    def _read_tab(self, handle, status_kwargs):
        """Read TSV file, absorbing encoding errors, and allowing for duplicate column names"""
        byte_tee = BytesIO(handle.read())
        reader_kwargs = dict(
            sep="\t", comment="#", header=None, index_col=False,
        )
        try:
            raw_tab = read_csv(byte_tee, **reader_kwargs)
        except (UnicodeDecodeError, ParserError) as e:
            byte_tee.seek(0)
            string_tee = StringIO(byte_tee.read().decode(errors="replace"))
            raw_tab = read_csv(string_tee, **reader_kwargs)
            update_status(
                **status_kwargs, report_type="parser message", status="warning",
                error=e, warning="Absorbing exception when parsing file",
            )
        raw_tab.columns = raw_tab.iloc[0,:]
        raw_tab.columns.name = None
        return raw_tab.drop(index=[0]).drop_duplicates().reset_index(drop=True)
