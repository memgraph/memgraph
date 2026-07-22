from enum import Enum


class Parameter(Enum):
    ID = "id"
    LABELS = "labels"
    PROPERTIES = "properties"
    TYPE = "type"
    END = "end"
    LABEL = "label"
    START = "start"
    NODE = "node"
    RELATIONSHIP = "relationship"
    DURATION = "duration("
    LOCALTIME = "localTime("
    LOCALDATETIME = "localDateTime("
    DATE = "date("
    STANDARD_INDENT = 4
