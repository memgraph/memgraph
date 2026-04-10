from typing import Any, Dict, Optional

DEFAULT_CONFIG = {
    "handleVocabUris": "SHORTEN",
    "handleMultival": "OVERWRITE",
    "handleRDFTypes": "LABELS",
    "keepLangTag": False,
    "keepCustomDataTypes": False,
    "typesToLabels": True,
}

VALID_HANDLE_VOCAB_URIS = {"SHORTEN", "IGNORE", "MAP", "KEEP"}
VALID_HANDLE_MULTIVAL = {"OVERWRITE", "ARRAY"}
VALID_HANDLE_RDF_TYPES = {"LABELS", "NODES", "LABELS_AND_NODES"}


class GraphConfig:
    _config: Optional[Dict[str, Any]] = None

    @classmethod
    def init(cls, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if cls._config is not None:
            raise RuntimeError("Graph config already initialized. Call graphconfig_drop first.")
        cls._config = dict(DEFAULT_CONFIG)
        if params:
            cls._validate_and_merge(params)
        return dict(cls._config)

    @classmethod
    def set(cls, params: Dict[str, Any]) -> Dict[str, Any]:
        if cls._config is None:
            raise RuntimeError("Graph config not initialized. Call graphconfig_init first.")
        cls._validate_and_merge(params)
        return dict(cls._config)

    @classmethod
    def show(cls) -> Dict[str, Any]:
        if cls._config is None:
            raise RuntimeError("Graph config not initialized. Call graphconfig_init first.")
        return dict(cls._config)

    @classmethod
    def drop(cls) -> None:
        cls._config = None

    @classmethod
    def is_initialized(cls) -> bool:
        return cls._config is not None

    @classmethod
    def get(cls, key: str, default: Any = None) -> Any:
        if cls._config is None:
            raise RuntimeError("Graph config not initialized. Call graphconfig_init first.")
        return cls._config.get(key, default)

    @classmethod
    def _validate_and_merge(cls, params: Dict[str, Any]) -> None:
        for key, value in params.items():
            if key not in DEFAULT_CONFIG:
                raise ValueError(f"Unknown config parameter: {key}")
            if key == "handleVocabUris" and value not in VALID_HANDLE_VOCAB_URIS:
                raise ValueError(
                    f"Invalid value for handleVocabUris: {value}. " f"Must be one of {VALID_HANDLE_VOCAB_URIS}"
                )
            if key == "handleMultival" and value not in VALID_HANDLE_MULTIVAL:
                raise ValueError(
                    f"Invalid value for handleMultival: {value}. " f"Must be one of {VALID_HANDLE_MULTIVAL}"
                )
            if key == "handleRDFTypes" and value not in VALID_HANDLE_RDF_TYPES:
                raise ValueError(
                    f"Invalid value for handleRDFTypes: {value}. " f"Must be one of {VALID_HANDLE_RDF_TYPES}"
                )
            if key in ("keepLangTag", "keepCustomDataTypes", "typesToLabels"):
                if not isinstance(value, bool):
                    raise ValueError(f"{key} must be a boolean, got {type(value)}")
            cls._config[key] = value
