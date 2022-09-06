var basic_formatter = function(r,c,v,d,x) {
    return ((v === true) || (v === "True"))
            ? "<font color=green>True</font>" : (
                ((v === false) || (v === "False"))
                ? "<font color='#FAA'>False</font>" : (
                    ((v === null) || (v === "NaN") || (v !== v))
                    ? "<i style='color:#BBB'>NaN</i>" : v ))
};

var columns = [], _rc = $COLUMNDATA;
for (var i = _rc.length; i --> 0;)
    columns[i] = {
        id: i, field: i, columnGroup: _rc[i][0], name: _rc[i][1],
        sortable: true, resizable: false, momentarilyFormattable: true,
        formatter: basic_formatter, defaultFormatter: basic_formatter,
    };
var data = $ROWDATA;

var context_url = "$CONTEXTURL";
var fr_assays = function(v, postfix) {
    return ((v === null) || (v === "NaN") || (v !== v))
        ? "<i style='color:#BBB'>NaN</i>"
        : (((v === false) || (v === "False"))
            ? "<font style='color:#FAA'>False</font>"
            : "<a class='filter' style='color:green' href='" +
              context_url + escape(postfix) + "'>True</a>");
}
var fr_samples = function(v, postfix) {
    return ((v === null) || (v === "NaN") || (v !== v))
        ? "<i style='color:#BBB'>NaN</i>"
        : "<a class='filter' href='" +
          context_url + escape(postfix) + "=" + escape(v) + "'>" + v + "</a>";
}

$FORMATTERS

var options = {
    createPreHeaderPanel: true, showPreHeaderPanel: true,
    defaultColumnWidth: 120, multiColumnSort: true,
    enableTextSelectionOnCells: true, enableColumnReorder: false,
    frozenColumn: $FROZENCOLUMN,
};
options.defaultFrozenColumn = options.frozenColumn;

function render_header_groups(pre_header_panel, start, end) {
    pre_header_panel
        .empty() .addClass("slick-header-columns")
        .css("left", "-1000px") .width(grid.getHeadersWidth());
    pre_header_panel.parent().addClass("slick-header");
    var hc_width_delta = grid.getHeaderColumnWidthDiff();
    var m, header, last_group = "", total_width = 0;
    for (var i = start; i < end; i++) {
        m = columns[i];
        if (last_group === m.columnGroup && i > 0) {
            total_width += m.width;
            header.width(total_width - hc_width_delta);
        }
        else {
            total_width = m.width;
            var mcg = m.columnGroup || "";
            header = $("<div class='ui-state-default slick-header-column' />")
                .html("<span class='slick-column-name'>" + mcg + "</span>")
                .width(m.width - hc_width_delta)
                .appendTo(pre_header_panel);
        }
        last_group = m.columnGroup;
    }
}

for (var i = columns.length; i --> 0;) {
    c = columns[i];
    var field = c.field;
    var longest = c.name;
    for (var j = data.length; j --> 0;) {
        var value = data[j][field];
        if ((value !== null) && (value === value) && (value !== undefined))
            if (value.toString().length > longest.length)
                longest = value;
    }
    var widthchecker = $("<span>"+longest+"&nbsp;&times;</span>").hide();
    $("body").append(widthchecker);
    columns[i].width = widthchecker.width() + 40;
    widthchecker.remove();
}

var grid = new Slick.Grid("#grid", data, columns, options);

if (options.frozenColumn !== undefined) {
    var b = options.frozenColumn + 1;
    render_header_groups($(grid.getPreHeaderPanelLeft()), 0, b);
    render_header_groups($(grid.getPreHeaderPanelRight()), b, columns.length);
}
else {
    render_header_groups($(grid.getPreHeaderPanel()), 0, columns.length);
}

var momentarily_formatted = [];

grid.onMouseEnter.subscribe(function (e, args) {
    var g = args.grid;
    var cell = g.getCellFromEvent(e);
    var r = cell.row, c = cell.cell;
    for (var x = 0, cl = columns.length; x < cl; x++) {
        if (momentarily_formatted[x] === true) {
            momentarily_formatted[x] = false;
            columns[x].formatter = columns[x].defaultFormatter;
            for (var y = 0, dl = data.length; y < dl; y++)
                g.updateCell(y, x);
        }
    }
    var col = columns[c], mfc = momentarily_formatted[c];
    if ((col.momentarilyFormattable) || (mfc !== undefined)) {
        momentarily_formatted[c] = true;
        var value = data[r][columns[c].field];
        columns[c].formatter = function(r,c,v,d,t) {
            var fmt = columns[c].defaultFormatter(r,c,v,d,t)
            return (v == value)
                ? "<font style='background:#FF8'>"+fmt+"</font>" : fmt;
        }
        for (var y = 0, dl = data.length; y < dl; y++)
            g.updateCell(y, c);
    }
});

grid.onSort.subscribe(function(e, args) {
    var sortcols = args.sortCols, g = args.grid;
    data.sort(function (r1, r2) {
        for (var i = 0, sl = sortcols.length; i < sl; i++) {
            var f = sortcols[i].sortCol.field;
            var v1 = r1[f], v2 = r2[f];
            var sign = sortcols[i].sortAsc ? 1 : -1;
            var ans = sign * (
                ((v1 === "NaN") || (v1 === null) || (v1 !== v1))
                ? (((v2 === "NaN") || (v2 === null) || (v2 !== v2)) ? 0 : -1)
                : (((v2 === "NaN") || (v2 === null) || (v2 !== v2))
                    ? 1 : v1.toString().localeCompare(
                        v2, undefined, {numeric: true}
                    )
                )
            );
            if (ans != 0) return ans;
        }
        return 0;
    });
    g.invalidate();
    g.render();
    render_header_groups(
        $(g.getPreHeaderPanelLeft()), 0, options.frozenColumn+1,
    );
    $("#sorting_popup").show();
    $("#sorting_reload").click(function (e) {location.reload()});
    $("#sorting_explain").click(function (e) {alert(
        "Data was sorted manually in the browser; the order may be different to when requested via other formats.\n" +
        "Static, non-interactive formats (CSV, TSV, JSON, CLS, GCT) always return data in the original order.\n" +
        "Please rely on the original order if using these data for analyses, as it guarantees consistency."
    )});
});

$(document).ready(function() {
    var ch = $("#crosshair-h"), cv = $("#crosshair-v");
    $(this).on("mousemove touchmove", function(e) {
        ch.css("top", e.pageY); cv.css("left", e.pageX);
    });
    $("#crosshair-toggle").click(function () {
        $(".crosshair").toggle();
    });
});

$(window).resize(function() {
    grid.setOptions({frozenColumn: false});
    grid.setOptions({frozenColumn: options.defaultFrozenColumn});
});