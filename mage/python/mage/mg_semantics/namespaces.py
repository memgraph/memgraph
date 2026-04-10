import re
from typing import Dict, List, Optional, Tuple

COMMON_PREFIXES = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dct": "http://purl.org/dc/terms/",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "schema": "http://schema.org/",
    "vcard": "http://www.w3.org/2006/vcard/ns#",
    "void": "http://rdfs.org/ns/void#",
    "dcat": "http://www.w3.org/ns/dcat#",
    "prov": "http://www.w3.org/ns/prov#",
    "sh": "http://www.w3.org/ns/shacl#",
}


class NamespaceManager:
    _prefixes: Dict[str, str] = {}

    @classmethod
    def reset(cls) -> None:
        cls._prefixes = {}

    @classmethod
    def init_common_prefixes(cls) -> None:
        cls._prefixes = dict(COMMON_PREFIXES)

    @classmethod
    def add(cls, prefix: str, namespace: str) -> Tuple[str, str]:
        if not prefix or not namespace:
            raise ValueError("Both prefix and namespace must be non-empty strings")
        if not namespace.endswith(("/", "#")):
            raise ValueError("Namespace URI must end with '/' or '#'")
        cls._prefixes[prefix] = namespace
        return prefix, namespace

    @classmethod
    def list_all(cls) -> List[Tuple[str, str]]:
        return [(prefix, ns) for prefix, ns in sorted(cls._prefixes.items())]

    @classmethod
    def remove(cls, prefix: str) -> None:
        if prefix not in cls._prefixes:
            raise ValueError(f"Prefix '{prefix}' not found")
        del cls._prefixes[prefix]

    @classmethod
    def remove_all(cls) -> None:
        cls._prefixes = {}

    @classmethod
    def add_from_text(cls, text: str) -> List[Tuple[str, str]]:
        added = []
        patterns = [
            r"@prefix\s+(\w+)\s*:\s*<([^>]+)>\s*\.",
            r"PREFIX\s+(\w+)\s*:\s*<([^>]+)>",
            r'xmlns:(\w+)\s*=\s*"([^"]+)"',
        ]
        for pattern in patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                prefix, namespace = match.group(1), match.group(2)
                cls._prefixes[prefix] = namespace
                added.append((prefix, namespace))
        return added

    @classmethod
    def shorten_uri(cls, uri: str) -> str:
        for prefix, namespace in cls._prefixes.items():
            if uri.startswith(namespace):
                local_name = uri[len(namespace) :]
                return f"{prefix}__{local_name}"
        return _get_local_name(uri)

    @classmethod
    def expand_uri(cls, short_form: str) -> str:
        if "__" in short_form:
            prefix, local_name = short_form.split("__", 1)
            if prefix in cls._prefixes:
                return cls._prefixes[prefix] + local_name
        return short_form

    @classmethod
    def get_prefixes(cls) -> Dict[str, str]:
        return dict(cls._prefixes)


def _get_local_name(uri: str) -> str:
    if "#" in uri:
        return uri.rsplit("#", 1)[1]
    if "/" in uri:
        return uri.rsplit("/", 1)[1]
    return uri


def _get_namespace(uri: str) -> str:
    if "#" in uri:
        return uri.rsplit("#", 1)[0] + "#"
    if "/" in uri:
        return uri.rsplit("/", 1)[0] + "/"
    return uri
