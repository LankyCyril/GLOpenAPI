var url_root = "$URL_ROOT";
var prefill_example = false;

var metadata = {
    // "wildcard": $METADATA_WILDCARDS,
    // "existence": $METADATA_EXISTENCE,
    // "equals": $METADATA_EQUALS,
    // TODO: this whole system is outdated - we will use /metadata-counts/
    //       in the near future
    "assay": $METADATA_ASSAYS,
    "file-datatype": $METADATA_DATATYPES,
};

function trimmed_option(key) {
    var strkey = key.toString()
    if (strkey[0] === "/")
        strkey = strkey.substring(1)
    if (strkey.length <= 64)
        return strkey;
    else
        return strkey.substring(0, 61) + "...";
}

function refresh_all_dropdowns() {
    for (key in metadata) {
        var section_lists = $("#selectors").find(".meta-" + key + ".list-item");
        section_lists.each(function () {
            var section_key = key;
            var section_data = metadata[section_key];
            for (var i = 0;; i++) {
                var section = $(this).find("select.level-"+i);
                if ((section.length > 0) && (section_data !== undefined)) {
                    var current_value = section.val();
                    if (current_value in section_data) {
                        section_key = current_value;
                        section_data = section_data[section_key];
                    }
                    else {
                        section.find("option").remove();
                        for (section_key in section_data) {
                            var key_as_shown = trimmed_option(section_key);
                            var option = new Option(key_as_shown, section_key);
                            $(option).html(key_as_shown);
                            section.append(option);
                            section.val(section_key);
                        }
                        section_data = section_data[section_key];
                    }
                }
                else {
                    break;
                }
            }
        });
    }
}

function refresh_dropdown_section(initiator, key, level) {
    var section_lists = $(initiator).parent();
    section_lists.each(function () {
        var section_key = key;
        if (section_key in metadata) {
            var section_data = metadata[section_key];
            for (var i = 0; i <= level; i++) {
                section_key = $(this).find("select.level-"+i).val();
                section_data = section_data[section_key];
            }
            for (var i = level+1;; i++) {
                var section = $(this).find("select.level-"+i);
                section.find("option").remove();
                for (section_key in section_data) {
                    var key_as_shown = trimmed_option(section_key);
                    var option = new Option(key_as_shown, section_key);
                    $(option).html(key_as_shown);
                    section.append(option);
                    section.val(section_key);
                }
                if ((section_data !== undefined)
                 && (section_data !== true)
                 && (section_key in section_data)) {
                    section_data = section_data[section_key];
                }
                else {
                    break;
                }
            }
        }
    });
}

function refresh_dropdowns(initiator) {
    var key = null, level = null;
    var key_class = $(initiator).parent().attr("class");
    if (key_class !== undefined) {
        var key_matcher = key_class.match(/meta-(\S+)/);
        if (key_matcher !== null) {
            key = key_matcher[1];
        }
    }
    var level_class = $(initiator).attr("class");
    if (level_class !== undefined) {
        var level_matcher = level_class.match(/level-(\d+)/);
        if (level_matcher !== null) {
            level = parseInt(level_matcher[1]);
        }
    }
    if ((key === null) || (level === null)) {
        refresh_all_dropdowns();
    }
    else {
        refresh_dropdown_section(initiator, key, level);
    }
}

function housekeeping(initiator) {
    if ($("#view").val() === "")
        $("#view").val("samples");
    $("#view option[value='']").remove();
    var first_assay_remover = $(
        "#meta-assay-list .list-item:first-child .remover"
    );
    first_assay_remover.removeClass("pipe-remover").html("&amp;");
    refresh_dropdowns(initiator);
}

function strip_special(s) {
    return s.replace(/(^[\/\?]+)|([\/\?]+$)/g, "");
}

function extract_full_base_url(a) {
    return (
        strip_special(a.prop("protocol")) + "//" +
        strip_special(a.prop("hostname")) +
        (a.prop("port") === "" ? "" : ":" + a.prop("port")) + "/"
    )
}

function desaturate_escaped_spaces(s) {
    return s.replace(/(\%..)/g, "<tt class='desaturated'>$1</tt>");
}

function decorate_url(url) {
    var a = $("<a>", {href: url});
    var decorated = (
        "<tt class='base_url'>" + extract_full_base_url(a) +
        "</tt><tt class='view'>" + strip_special(a.prop("pathname")) +
        "</tt><tt class='pale'>/</tt>"
    );
    var tail = strip_special(a.prop("search"));
    var level = 0, sep = "?", at_alphanumeric_start = true;
    while (tail) {
        var match = /[\.\|\&\=]/.exec(tail);
        if (match === null) {
            var part = tail;
            tail = "";
        }
        else {
            var part = tail.substring(0, match.index);
            tail = tail.substring(match.index + 1);
        }
        if ((sep === ".") || (sep === "="))
            decorated += "<tt class='plain'>" + sep + "</tt>";
        else
            decorated += "<tt class='sep'>" + sep + "</tt>";
        if (part.length > 0) {
            at_alphanumeric_start = false;
            decorated += (
                "<tt class='part-level-" + level + "'>" +
                desaturate_escaped_spaces(part) + "</tt>"
            );
        }
        if (match !== null)
            sep = match[0];
        if (sep === "&")
            level = 0;
        else if (sep === "=")
            level = at_alphanumeric_start? 0 : 3;
        else if ((sep !== "|") && (level < 4))
            level += 1;
    }
    return decorated;
}

var TARGETS = [
    // "wildcard", "existence", "equals",
    // TODO: this whole system is outdated - we will use /metadata-counts/
    //       in the near future
    "assay", "file-filename-pseudo", "file-datatype-pseudo", "file-datatype",
    "format", "schema", "debug",
];

function generate_url() {
    housekeeping(initiator=this);
    var url = url_root + "/" + escape($("#view").val()) + "/?";
    for (var i = 0, l = TARGETS.length; i < l; i++) {
        var prefix = $(".meta-"+TARGETS[i]+":first-child .included-key").text();
        url += prefix;
        $(".meta-" + TARGETS[i]).each(function () {
            if ($(this).children("input[type=checkbox]").prop("checked")) {
                $(this).find(".key-level").each(function () {
                    var this_val = $(this).val().toString();
                    if (this_val[0] === "/")
                        url = url.replace(/\.+$/g, "");
                    if ($(this).hasClass("level-noescape"))
                        url += $(this).val();
                    else
                        url += escape($(this).val());
                    if (!$(this).hasClass("level-nodot"))
                        url += ".";
                });
                var keyunion_level_dropdowns = $(this).find(".keyunion-level");
                if (keyunion_level_dropdowns.length > 0) {
                    keyunion_level_dropdowns.each(function () {
                        url = url + escape($(this).val()) + "|";
                    });
                    url = url.replace(/\|$/g, "&");
                }
                else if (TARGETS[i] == "assay")
                    url = url.replace(/\.+$/g, "|");
                else
                    url = url.replace(/\.+$/g, "&");
                var value_level_dropdowns = $(this).find(".value-level");
                if (value_level_dropdowns.length > 0) {
                    url = url.replace(/&$/g, "=");
                    value_level_dropdowns.each(function () {
                        url += escape($(this).val()) + "|";
                    });
                    url = url.replace(/\|$/g, "&");
                }
            }
        });
        url = url.replace(RegExp(prefix+"$"), "").replace(/\|$/, "&");
    }
    $("#url-description").text("Generated URL:");
    $("#url").html(decorate_url(url));
    $("#url-follow a").attr("href", $("#url").text());
    $("#url-follow").css("background", "lightgreen");
}

function toggle_builder() {
    $(".interactive").show();
    $(".on-newline").css("display", "block");
    $("#noscript").hide();
    var offset = document.getElementById("url-builder").offsetHeight;
    $("a[name]:not(.beforebuilder)").each(function () {
        $(this).css("scroll-margin-top", offset);
    });
}

function set_default_repr_of_remover_ors() {
    var i = 0;
    $(".remover-or").each(function () {
        if (++i == 1)
            $(this).html("&amp;");
        else
            $(this).html("|");
    });
}

function remove_last_query_option() {
    $(this).prev().prev().remove();
    $(this).prev("select").remove();
    $(this).remove();
    generate_url();
}

function remove_query_part() {
    if ($(this).parents(".list").children(".list-item").length > 1)
        $(this).parents(".list-item").remove();
    else
        $(this).parents(".list-item")
            .children("input[type=checkbox]")
            .prop("checked", false);
    generate_url();
}

function clone_query_part() {
    var section_name = $(this).attr("class").replace(/\sadd$/g, "");
    var section_list = $("#"+section_name+"-list");
    var section_clone = section_list.children().last("."+section_name).clone();
    if (section_name === "meta-assay") {
        section_clone.children("em.remover").addClass("pipe-remover").html("|");
    }
    section_list.append(section_clone);
    refresh_event_handlers();
    generate_url();
}

function clone_query_or() {
    var section_name = $(this).attr("class").replace(/\sor$/g, "");
    var or_selects = $(this).parent("div").children(".or-selects");
    var new_dropdown = or_selects.children("select").last().clone();
    or_selects.append("<em style='margin-left:3pt;margin-right:3pt'>|</em>");
    or_selects.append(new_dropdown);
    or_selects.append("<a class='del'>x</a>");
    refresh_event_handlers();
    generate_url();
}

function refresh_event_handlers() {
    $(".remover, .remover-or")
        .unbind("mouseover")
        .unbind("mouseout")
        .unbind("click");
    $(".remover, .remover-or").mouseover(function () {$(this).html("&times;")});
    $(".remover").mouseout(function () {
        if ($(this).hasClass("pipe-remover"))
            $(this).html("|");
        else
            $(this).html("&amp;");
    });
    set_default_repr_of_remover_ors();
    $(".remover-or").mouseout(set_default_repr_of_remover_ors);
    $(".remover, .remover-or").click(remove_query_part);
    $("a.add, a.or, a.del, input[type=checkbox], select").unbind("click");
    $("a.add").click(clone_query_part);
    $("a.del").click(remove_last_query_option);
    $("a.or").click(clone_query_or);
    $("input[type=checkbox], select").click(generate_url);
    $("input[type=text]").on("change", generate_url);
    $("input[type=text]").on("input", generate_url);
}

function do_prefill_example() {
    var initiator = $("#meta-wildcard-list .key-level.level-0")
    initiator.val("study");
    refresh_dropdown_section(initiator, "wildcard", 0);
    $("#meta-wildcard-list .key-level.level-1").val("characteristics");
    initiator = $("#meta-existence-list .key-level.level-0")
    initiator.val("study");
    refresh_dropdown_section(initiator, "existence", 0);
    initiator = $("#meta-existence-list .key-level.level-1")
    initiator.val("factor value");
    refresh_dropdown_section(initiator, "existence", 1);
    $("#meta-existence-list .keyunion-level.level-2").val("spaceflight");
    initiator = $("#meta-equals-list .key-level.level-0");
    initiator.val("investigation");
    refresh_dropdown_section(initiator, "equals", 0);
    initiator = $("#meta-equals-list .key-level.level-1");
    initiator.val("study assays");
    refresh_dropdown_section(initiator, "equals", 1);
    initiator = $("#meta-equals-list .key-level.level-2");
    initiator.val("study assay technology type");
    refresh_dropdown_section(initiator, "equals", 2);
    $("#meta-equals-list .value-level.level-3").val("RNA Sequencing (RNA-Seq)");
    $(".meta-assay.add").click();
    initiator = $("#meta-assay-list .key-level.level-0")
    initiator.val("GLDS-168");
    refresh_dropdown_section(initiator, "assay", 0);
    $("#meta-assay-list .key-level.level-1").val("");
    initiator = $("#meta-assay-list .list-item:first-child .key-level.level-0")
    initiator.val("GLDS-48");
    refresh_dropdown_section(initiator, "assay", 0);
    $("#meta-assay-list .list-item:first-child .key-level.level-1").val("");
    $("#meta-file-datatype-list .value-level").val("unnormalized counts");
    $("#url-description").text("You are here:");
    $("#url").html("$URL_ROOT");
    $("#url-follow a").attr("href", "");
    $("#url-follow").css("background", "#DDD");
    var option = new Option("", "");
    $(option).html("");
    $("select#view").append(option).val("");
}

$(document).ready(function() {
    toggle_builder();
    refresh_dropdowns();
    refresh_event_handlers();
    if (prefill_example)
        do_prefill_example();
});
