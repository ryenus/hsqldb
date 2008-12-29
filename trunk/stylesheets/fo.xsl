<?xml version='1.0'?>
<!-- $Id$ -->
<!-- See http://www.sagehill.net/docbookxsl/CustomDb5Xsl.html for general
     syntax. -->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
     xmlns:fo="http://www.w3.org/1999/XSL/Format"
     xmlns:d="http://docbook.org/ns/docbook" exclude-result-prefixes="d">

  <xsl:import
    href="http://docbook.sourceforge.net/release/xsl-ns/current/fo/docbook.xsl"/>

  <!-- See http://www.sagehill.net/docbookxsl/BordersAndShading.html -->
  <xsl:attribute-set name="admonition.properties">
    <xsl:attribute name="border">0.5pt solid black</xsl:attribute>
    <xsl:attribute name="background-color">#E0E0E0</xsl:attribute>
    <xsl:attribute name="padding">0.1in</xsl:attribute>
  </xsl:attribute-set>

  <!-- See http://docbook.sourceforge.net/release/xsl-ns/current/doc/html/shade.verbatim.style.html -->
  <xsl:attribute-set name="shade.verbatim.style">
    <xsl:attribute name="border">0.5pt solid gray</xsl:attribute>
    <xsl:attribute name="background-color">#E0E0E0</xsl:attribute>
    <!-- Note that some of OASIS's examples the long-deprecated "bgcolor".
         You should use "background-color". -->
  </xsl:attribute-set>
</xsl:stylesheet> 
