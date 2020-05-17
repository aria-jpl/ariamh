'''
A simple utility that convertsn json to xml and vice-versa

'''

from collections import OrderedDict
import xml.etree.cElementTree as ET
from xml.dom import minidom
import sys, json


GROUP_TAG="group"
SCALAR_TAG="scalar"
VECTOR_TAG="vector"
ELEMENT_TAG="element"


def convert_group(root, group_name, group_dict):
    """
    Convert a RunConfig group coming from the context as a dict object
    into a group sub tree in xml
    :param group_name:
    :param group_dict:
    :return:
    """
    group_root = ET.SubElement(root, GROUP_TAG)
    group_root.set("name", group_name)
    child = None
    for key in group_dict:
        value = group_dict[key]
        if type(value) == type(list()):
            child = ET.SubElement(group_root, VECTOR_TAG)
            child.set("name", key)
            for elem in value:
                element = ET.SubElement(child, ELEMENT_TAG)
                element.text = str(elem)
        elif type(value) == type(str()):
            child = ET.SubElement(group_root, SCALAR_TAG)
            child.set("name", key)
            child.text = str(value)
    return root


def dict2xml(python_dict, namespaces=None, version=1.0, encoding="UTF-8"):
    """
    Convert python dict object to xml
    :param python_dict: dictionary object
    :param namespaces: List of (prefix, uri) tuples
    :param version: xml version
    :param encoding: xml document encoding
    :return: xml string
    """

    if namespaces is not None:
        for namespace in namespaces:
            (prefix, uri) = namespace
            ET.register_namespace(prefix, uri)
    xmlroot = ET.Element("input")
    for key in python_dict:
        convert_group(xmlroot, key, python_dict[key])

    return ET.ElementTree(xmlroot)


def dict2xmlstring(python_dict, namespaces=None, version=1.0, encoding="UTF-8"):
    xmlroot = dict2xml(python_dict, namespaces, version, encoding).getroot()
    return xml2string(xmlroot)


def xml2string(xmlroot, encoding="UTF-8", method="xml", indent="", newl=""):
    xmlstring = minidom.parseString(ET.tostring(xmlroot, encoding=encoding, method=method))\
        .toprettyxml(newl=newl, indent=indent)
    return xmlstring


def json2xml(jsonfile):
    """
    Convert the json file to xml
    """
    jsondict = simplejson.load(open(jsonfile).read())
    return dict2xml(jsondict)


def printChildren(root):
    root_dict = OrderedDict()
    for child in root:
        if child.tag == 'component':
            group = OrderedDict()

            for c in child:
                if c.tag == 'vector':
                    elems = []
                    for e in c:
                        elems.append(e.text)
                    group.update({c.attrib.get('name'): elems})
                else:
                    group.update({c.attrib.get('name'): c.text})
            root_dict.update({child.attrib.get('name'): group})
    return root_dict


def xml2json(xmlfile):
    import xml
    root = xml.etree.ElementTree.parse(xmlfile).getroot()
    output = {}
    return printChildren(root)


def main():
    if len(sys.argv) != 2:
        print("Usage : python xml_json_converter.py <xml_file>")
        exit(0)

    resp = xml2json(sys.argv[1])
    print((json.dumps(resp, indent=2)))

if __name__ == '__main__':
    main()
