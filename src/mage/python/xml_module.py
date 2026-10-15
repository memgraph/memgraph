import urllib.request

import defusedxml.ElementTree as ET
import mgp

TYPE = "_type"
TEXT = "_text"
CHILDREN = "_children"


def parse_element(element, simple):
    result = {TYPE: element.tag}

    attributes = element.attrib
    if attributes:
        result.update(attributes)

    text_content = element.text
    if text_content and text_content.strip():
        result[TEXT] = text_content

    children = list(element)
    if children:
        children_name = CHILDREN
        if simple:
            children_name = "_" + str(element.tag)
        result[children_name] = [parse_element(child, simple) for child in children]

    return result


def xml_file_to_string(xml_file):
    xml_string = ""
    try:
        with open(xml_file, "r") as xml_file:
            xml_string = xml_file.read().replace("\n", "").replace("  ", "")
    except PermissionError:
        raise PermissionError(
            "You don't have permissions to write into that file."
            "Make sure to give the necessary permissions to user memgraph."
        )
    except Exception:
        raise OSError("Could not open or write to file.")
    return xml_string


@mgp.function
def parse(xml_input: str, simple: bool = False, path: str = "") -> mgp.Map:
    """
    Function to parse xml string (or file) into a map.

    Parameters
    ----------
    xml_input : str
        XML string which is to be parsed.
    simple: bool = false
        Boolean which specifies how should the children list be named,
        when it is false, all children lists are named _children
        if true, all children lists are named based on their parent.
    path: str = ""
        Path to XML file which is to be parsed, if it is not "",
        XML string is ignored and only file is parsed, if it is left as
        default, it is ignored.

    Returns:
        mgp.Map -> XML file parsed as map
    """

    root = None
    parser = ET.DefusedXMLParser()
    if path:
        root = ET.fromstring(xml_file_to_string(path), parser)
    else:
        root = ET.fromstring(xml_input, parser)
    output_map = parse_element(root, simple)
    return output_map


def check_url(url):
    if not url.endswith(".xml"):
        raise ValueError("File must be xml!")


def xpath_search(root, xpath_expression):
    try:
        result = root.findall(xpath_expression)
        return result
    except Exception as e:
        raise ValueError(f"XPath search error: {e}")


"""
Note: our implementation of xpath is different from neo4j,
because python offers different support for xpath than java
We cant have absolute paths
And our xpath search starts from the root node,
so ./something is equivalent to /root/something
For example,
python input : .//ZONE is equivalent to java input //ZONE
https://docs.python.org/3/library/xml.etree.elementtree.html#xpath-support
here are our avaliable xpath options
"""


@mgp.read_proc
def load(
    xml_url: str,
    simple: bool = False,
    path: str = "",
    xpath: str = "",
    headers: mgp.Map = {},
) -> mgp.Record(output_map=mgp.Map):
    """
    Procedure to load XML from url or from file to a map.

    Parameters
    ----------
    xml_url : str
        Url of the xml file to be parsed.
    simple: bool = false
        Boolean which specifies how should the children list be named,
        when it is false, all children lists are named _children
        if true, all children lists are named based on their parent.
    path: str = ""
        Path to XML file which is to be parsed, if it is not "",
        XML url is ignored and only file is parsed. If it is left as default,
        file path is ignored.
    xpath: str = ""
        Xpath expression which specifies which elements shall be returned.
        If left as "", it will be ignored, otherwise,
        only elements in XML file which satisfy
        expression are returned.
    headers: mgp.Map ={}
        Additional HTTP headers used in url request.

    Returns:
        mgp.Map -> XML file or URL parsed as map.
        In case XPATH is active, a map of for each element will be returned.
    """
    root = None
    parser = ET.DefusedXMLParser()
    if path:
        root = ET.fromstring(xml_file_to_string(path), parser)
    else:
        check_url(xml_url)
        try:
            request = urllib.request.Request(xml_url, headers=headers)
            response = urllib.request.urlopen(request).read()
            root = ET.fromstring(response)
        except Exception as e:
            raise ValueError(f"Error while fetching or parsing XML: {e}")

    if xpath:
        record_list = list()
        xpath_list = xpath_search(root, xpath)
        for element in xpath_list:
            record_list.append(mgp.Record(output_map=parse_element(element, simple)))
        return record_list
    output_map = parse_element(root, simple)
    return mgp.Record(output_map=output_map)
