#!/usr/bin/env python
# encoding: utf8

import base64

xml_escape_table = {
    "&": "&amp;",
    '"': "&quot;",
#    "'": "&apos;",
    ">": "&gt;",
    "<": "&lt;",
    }

def xml_escape(text):
    """Produce entities within text."""
    return "".join(xml_escape_table.get(c,c) for c in text)

def addNode(doc, nodeName, nodeValue):
    doc.startNode(nodeName)
    doc.putText(nodeValue)
    doc.endNode()

#-------------------------------------------------------------
class SimpleXmlConstructor:
    def __init__(self, shiftLevel=0):
        self._shiftLevel = shiftLevel
        self.buf = ""
        self._items = []
        self._attributes = []
        self._pendedStart = False
        self._curNodeName = ""

    def includeEncoding(self, encoding="UTF-8"):
        self.buf += '<?xml version="1.0" encoding="{0}"?>\n'.format(encoding)
        
    def includeStylesheet(self, stylesheet):
        self.buf += '<?xml-stylesheet type="text/xsl" href="{0}"?>\n'.format(stylesheet)
        
    def asText(self):
        if len(self._items) > 0:
            raise Exception("no endNode")
        return self.buf
    
        
    def startNode(self, name):
        self._openNodeIfNeed()
        self._curNodeName = name
        self._attributes = []
        self._pendedStart = True
        self._items.append( name )
        
    def addAttribute(self, name, value):
        if isinstance(value, unicode):
            value = value.encode('utf8')
        value2 = xml_escape(value)
        self._attributes.append([name, value2])
    
    def endNode(self):
        self._openNodeIfNeed()
        self.buf += "</{0}>".format( self._items[ len(self._items)-1 ]  )
        del self._items[ len(self._items)-1 ]
    
    def putText(self, text):
        self._openNodeIfNeed()
        if isinstance(text, unicode):
            text = text.encode('utf8')
        text2 = xml_escape(text)
        self.buf += text2

    def putRawText(self, text):
        self._openNodeIfNeed()
        self.buf += text


    def importNode(self, subnode):
        self._openNodeIfNeed()
        assert isinstance(subnode, SimpleXmlConstructor)
        if subnode._shiftLevel >= 0:
            self.buf += "\n" + " "*subnode._shiftLevel
        self.buf += subnode.asText()

        
    def putTextBase64(self, text):
        self._openNodeIfNeed()
        if isinstance(text, unicode):
            text = text.encode('utf8')
        self.buf += base64.b64encode( text )
        
    def _openNodeIfNeed(self):
        if self._pendedStart:
            arr = []
            for n,v in self._attributes:
                arr.append( "{0}=\"{1}\"".format(n,v) )
                
            attr = " " if len(self._attributes) else ""
            attr += " ".join(arr)
            self._pendedStart = False
            
            if self._shiftLevel >= 0 and len(self._items)>1:
                self.buf += "\n" + (self._shiftLevel + len(self._items))*" "
        
            self.buf += "<{0}{1}>".format( self._curNodeName, attr )

        
        
                
        
    
        
if __name__ == '__main__':
    y = SimpleXmlConstructor()
    y.startNode("subnode")
    y.putText("texttexttext")
    y.endNode()
    
    
    x = SimpleXmlConstructor()
    x.startNode("a1")
    x.startNode("b1")
    x.addAttribute("a:b", "my-attr")
    x.addAttribute("a:c", "my-attr2")
    x.putText("bla1-")
    x.putTextBase64(u"по русски")
    x.putText("-bla1-")
    x.putTextBase64("some text")
    x.startNode("c1")
    x.putText(u"OOO\"Копыта\"")
    x.endNode()
    x.endNode()
    x.startNode("b2")
    x.importNode(y)
    x.endNode()
    
    x.endNode()
    
    print x.asText()
