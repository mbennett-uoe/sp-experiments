import xml.etree.ElementTree as ET
import os
import re
from time import sleep
from datetime import datetime
root_path = "./output/xml/"

def get_valid_filename(s):
    """
    Return the given string converted to a string that can be used for a clean
    filename. Remove leading and trailing spaces; convert other spaces to
    underscores; and remove anything that is not an alphanumeric, dash,
    underscore, or dot.
    >>> get_valid_filename("john's portrait in 2004.jpg")
    'johns_portrait_in_2004.jpg'
    """
    s = str(s).strip().replace(' ', '_')
    return re.sub(r'(?u)[^-\w.]', '', s)

def getfh(dir, filename, retry = False):
    dir = get_valid_filename(dir)
    path = root_path + dir + "/" + filename + ".xml"
    if os.path.exists(root_path + dir):
        try:
            fh = open(path, "r+")
            return fh
        except IOError:
            fh = open(path, "w+")
            return fh
        except OSError:
            if not retry:
                sleep(5)
                getfh(dir, filename, retry = True)
            else:
                return None

    else:
        os.makedirs(root_path + dir)
        getfh(dir, filename)

def writetree(shelfmark, index, tree):
    try:
        fh = getfh(shelfmark, index)
        ET.ElementTree(tree).write(fh, encoding="UTF-8", xml_declaration=True)
        return True
    except Exception as e:
        print e

def gettree(shelfmark, index):
    try:
        fh = getfh(shelfmark, index)
    except Exception as e:
        print e

    try:
        tree = ET.parse(fh)
        tree = tree.getroot()
    except ET.ParseError:
#        print "parsing error = generating new file"
        tree = createtree(shelfmark, index)
    except TypeError:
	print "TypeError, gen new file"
	tree = createtree(shelfmark,index)

    return tree

def createtree(shelfmark, index):
    doc = ET.Element('object')
    doc.set('shelfmark', shelfmark)
    doc.set('index', index)
    return doc


def additem(tree, sequence, title = None, origin = None):
    item = ET.Element('item', {"sequence": sequence})
    if title:
        t = ET.Element('title')
        t.text = title
        item.append(t)
    if origin:
        t = ET.Element('origin')
        t.text = origin
        item.append(t)
    tree.append(item)
    return tree

def addlog(tree, sequence, process, message):
    item = tree.find("./item[@sequence='%s']"%sequence)
    if item is None:
        item = ET.Element('item', {"sequence": sequence})
        tree.append(item)

    log = ET.Element("log")
    entry = ET.Element("entry")
    entry.set("timestamp", datetime.now().isoformat())
    proc = ET.Element("process")
    proc.text = process
    status = ET.Element("status")
    status.text = message
    entry.append(proc)
    entry.append(status)
    log.append(entry)
    item.append(log)

    return tree

def settitle(tree, sequence, title):
    item = tree.find("./item[@sequence='%s']" % sequence)
    if item is None:
        tree = additem(tree, sequence, title = title)
    else:
        item.text = title
    return tree

def addimage(tree, sequence, type, path):
    item = tree.find("./item[@sequence='%s']" % sequence)
    if item is None:
        item = ET.Element('item', {"sequence": sequence})
        tree.append(item)

    image = ET.Element('image', {'type': type})
    image.text = path

    item.append(image)

    return tree


def addocr(tree, sequence, type, language, path):
    item = tree.find("./item[@sequence='%s']" % sequence)
    if item is None:
        item = ET.Element('item', {"sequence": sequence})
        tree.append(item)

    ocr = ET.Element('ocr', {'type': type, 'language': language})
    ocr.text = path

    item.append(ocr)

    return tree

def getimage(tree, sequence, type):
    item = tree.find("./item[@sequence='%s']/image[@type='%s']" % (sequence, type))
    if item is None:
        return None
    else:
        return item.text

def getorigin(tree, sequence):
    item = tree.find("./item[@sequence='%s']/origin" % sequence)
    if item is None:
        return None
    else:
        return item.text

def setorigin(tree, sequence, origin):
    item = tree.find("./item[@sequence='%s']/origin" % sequence)
    if item is None:
        tree = additem(tree, sequence, origin = origin)
    else:
        item.text = origin
    return tree
