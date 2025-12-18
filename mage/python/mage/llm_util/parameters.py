from enum import Enum


class Parameter(Enum):
    END = "end"
    NODE_PROPS = "node_props"
    PROPERTY = "property"
    RELATIONSHIPS = "relationships"
    REL_PROPS = "rel_props"
    START = "start"
    TYPE = "type"


class OutputType(Enum):
    RAW = "raw"
    PROMPT_READY = "prompt_ready"
