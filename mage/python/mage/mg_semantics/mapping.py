from typing import Dict, List, Optional, Tuple


class MappingManager:
    # Maps DB element name -> full URI
    _mappings: Dict[str, str] = {}

    @classmethod
    def reset(cls) -> None:
        cls._mappings = {}

    @classmethod
    def add(cls, full_uri: str, db_name: str) -> Tuple[str, str]:
        if not full_uri or not db_name:
            raise ValueError("Both full_uri and db_name must be non-empty strings")
        cls._mappings[db_name] = full_uri
        return db_name, full_uri

    @classmethod
    def drop(cls, db_name: str) -> None:
        if db_name not in cls._mappings:
            raise ValueError(f"No mapping found for '{db_name}'")
        del cls._mappings[db_name]

    @classmethod
    def drop_all(cls, namespace: Optional[str] = None) -> None:
        if namespace is None:
            cls._mappings = {}
        else:
            to_remove = [name for name, uri in cls._mappings.items() if uri.startswith(namespace)]
            for name in to_remove:
                del cls._mappings[name]

    @classmethod
    def list_all(cls, filter_str: Optional[str] = None) -> List[Tuple[str, str]]:
        results = []
        for db_name, uri in sorted(cls._mappings.items()):
            if filter_str is None or filter_str in db_name or filter_str in uri:
                results.append((db_name, uri))
        return results

    @classmethod
    def uri_for_db_name(cls, db_name: str) -> Optional[str]:
        return cls._mappings.get(db_name)

    @classmethod
    def db_name_for_uri(cls, uri: str) -> Optional[str]:
        for name, mapped_uri in cls._mappings.items():
            if mapped_uri == uri:
                return name
        return None

    @classmethod
    def get_mappings(cls) -> Dict[str, str]:
        return dict(cls._mappings)
