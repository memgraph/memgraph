import re
from typing import Optional

from mage.mg_semantics.namespaces import NamespaceManager, _get_local_name, _get_namespace


class RdfUtils:
    @staticmethod
    def get_iri_local_name(uri: str) -> str:
        return _get_local_name(uri)

    @staticmethod
    def get_iri_namespace(uri: str) -> str:
        return _get_namespace(uri)

    @staticmethod
    def get_data_type(value: str) -> str:
        if "^^" in value:
            return value.rsplit("^^", 1)[1].strip("<>")
        return "http://www.w3.org/2001/XMLSchema#string"

    @staticmethod
    def get_lang_value(lang: str, value: str) -> Optional[str]:
        pattern = rf'"([^"]*)"@{re.escape(lang)}'
        match = re.search(pattern, value)
        if match:
            return match.group(1)
        if isinstance(value, str) and value.startswith(f"@{lang}:"):
            return value.split(":", 1)[1]
        parts = _parse_lang_tagged_values(value)
        for tag, val in parts:
            if tag == lang:
                return val
        return None

    @staticmethod
    def get_lang_tag(value: str) -> Optional[str]:
        match = re.search(r"@([a-zA-Z\-]+)\s*$", value)
        if match:
            return match.group(1)
        return None

    @staticmethod
    def has_lang_tag(lang: str, value: str) -> bool:
        tag = RdfUtils.get_lang_tag(value)
        return tag == lang

    @staticmethod
    def get_value(value: str) -> str:
        if "^^" in value:
            return value.rsplit("^^", 1)[0].strip('"')
        match = re.match(r'"([^"]*)"', value)
        if match:
            return match.group(1)
        return value

    @staticmethod
    def short_form_from_full_uri(uri: str) -> str:
        return NamespaceManager.shorten_uri(uri)

    @staticmethod
    def full_uri_from_short_form(short_form: str) -> str:
        return NamespaceManager.expand_uri(short_form)


def _parse_lang_tagged_values(value: str):
    results = []
    pattern = r'"([^"]*)"@([a-zA-Z\-]+)'
    for match in re.finditer(pattern, value):
        results.append((match.group(2), match.group(1)))
    return results
