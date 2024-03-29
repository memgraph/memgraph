#!/usr/bin/env python3
import argparse
import copy
import os
import subprocess
import sys
import textwrap
import xml.etree.ElementTree as ET

import yaml

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "flags.yaml")
WIDTH = 80


def wrap_text(s, initial_indent="# "):
    return "\n#\n".join(
        map(lambda x: textwrap.fill(x, WIDTH, initial_indent=initial_indent, subsequent_indent="# "), s.split("\n"))
    )


def extract_flags(binary_path):
    ret = {}
    data = subprocess.run([binary_path, "--help-xml"], stdout=subprocess.PIPE).stdout.decode("utf-8")
    # If something is printed out before the help output, it will break the the
    # XML parsing -> filter out if something is not XML line because something
    # can be logged before gflags output (e.g. during the global objects init).
    # This gets called during memgraph build phase to generate default config
    # file later installed under /etc/memgraph/memgraph.conf
    # NOTE: Don't use \n in the gflags description strings.
    # NOTE: Check here if gflags version changes because of the XML format.
    data = "\n".join([line for line in data.split("\n") if line.startswith("<")])
    root = ET.fromstring(data)
    for child in root:
        if child.tag == "usage" and child.text.lower().count("warning"):
            raise Exception("You should set the usage message!")
        if child.tag == "flag":
            flag = {}
            for elem in child:
                flag[elem.tag] = elem.text if elem.text is not None else ""
            flag["override"] = False
            ret[flag["name"]] = flag
    return ret


def apply_config_to_flags(config, flags):
    flags = copy.deepcopy(flags)
    for name in config["undocumented"]:
        flags.pop(name)
    for modification in config["modifications"]:
        name = modification["name"]
        if name not in flags:
            print("WARNING: Flag '" + name + "' missing from binary!", file=sys.stderr)
            continue
        flags[name]["default"] = modification["value"]
        flags[name]["override"] = modification["override"]
    return flags


def extract_sections(flags):
    sections = []
    other = []
    current_section = ""
    current_flags = []
    for name in sorted(flags.keys()):
        section = name.split("_")[0]
        if section == current_section:
            current_flags.append(name)
        else:
            if len(current_flags) < 2:
                other.extend(current_flags)
            else:
                sections.append((current_section, current_flags))
            current_section = section
            current_flags = [name]
    if len(current_flags) < 2:
        other.extend(current_flags)
    else:
        sections.append((current_section, current_flags))
    sections.append(("other", other))
    assert set(sum(map(lambda x: x[1], sections), [])) == set(
        flags.keys()
    ), "The section extraction algorithm lost some flags!"
    return sections


def generate_config_file(sections, flags):
    ret = wrap_text(config["header"]) + "\n\n\n"
    for section, section_flags in sections:
        ret += wrap_text(section.capitalize(), initial_indent="## ") + "\n\n"
        for name in section_flags:
            flag = flags[name]
            helpstr = flag["meaning"] + " [" + flag["type"] + "]"
            ret += wrap_text(helpstr) + "\n"
            prefix = "# " if not flag["override"] else ""
            ret += prefix + "--" + flag["name"].replace("_", "-") + "=" + flag["default"] + "\n\n"
        ret += "\n"
    ret += wrap_text(config["footer"])
    return ret.strip() + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("memgraph_binary", help="path to Memgraph binary")
    parser.add_argument("output_file", help="path where to store the generated Memgraph " "configuration file")
    parser.add_argument("--config-file", default=CONFIG_FILE, help="path to generator configuration file")

    args = parser.parse_args()
    flags = extract_flags(args.memgraph_binary)

    with open(args.config_file) as f:
        config = yaml.safe_load(f)

    flags = apply_config_to_flags(config, flags)
    sections = extract_sections(flags)
    data = generate_config_file(sections, flags)

    dirname = os.path.dirname(args.output_file)
    if dirname and not os.path.exists(dirname):
        os.makedirs(dirname)

    with open(args.output_file, "w") as f:
        f.write(data)
