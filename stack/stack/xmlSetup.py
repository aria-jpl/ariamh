#!/usr/bin/env python3
import xml.etree.ElementTree as ET

def indentXML(elem, depth = None,last = None):
    if depth == None:
        depth = [0]
    if last == None:
        last = False
    tab = ' '*4
    if(len(elem)):
        depth[0] += 1
        elem.text = '\n' + (depth[0])*tab
        lenEl = len(elem)
        lastCp = False
        for i in range(lenEl):
            if(i == lenEl - 1):
                lastCp = True
            indentXML(elem[i],depth,lastCp)
        if(not last):
            elem.tail = '\n' + (depth[0])*tab
        else:
            depth[0] -= 1
            elem.tail = '\n' + (depth[0])*tab
    else:
        if(not last):
            elem.tail = '\n' + (depth[0])*tab
        else:
            depth[0] -= 1
            elem.tail = '\n' + (depth[0])*tab

def XMLFromDict(ddict, name=None):
    '''
    Creates an XML file from dictionary. Derived from ISCE.
    '''
    if not name:
        name = ''
    root = ET.Element('component')
    root.attrib['name'] = name
    for key, val in ddict.items():
        if key.endswith('.catalog'):
            compSubEl = ET.SubElement(root, 'component')
            compSubEl.attrib['name'] = key[0:-8]
            ET.SubElement(compSubEl, 'catalog').text = str(val)

        elif (not isinstance(val, dict)):
            propSubEl = ET.SubElement(root,'property')
            propSubEl.attrib['name'] = key
            ET.SubElement(propSubEl, 'value').text = str(val)

        else:
            root.append(XMLFromDict(val, name=key))

    return root

def writeXML(file, root):
    '''
    Indents the XML object and writes it to file.
    '''

    indentXML(root)
    etObj = ET.ElementTree(root)
    etObj.write(file)
    return 
