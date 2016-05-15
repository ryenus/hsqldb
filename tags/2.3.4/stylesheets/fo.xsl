<?xml version='1.0'?>
<!-- $Id$ -->
<!-- See http://www.sagehill.net/docbookxsl/CustomDb5Xsl.html for general
     syntax. -->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
     xmlns:fo="http://www.w3.org/1999/XSL/Format"
     xmlns:d="http://docbook.org/ns/docbook" exclude-result-prefixes="d">

  <xsl:import
    href="http://docbook.sourceforge.net/release/xsl-ns/current/fo/docbook.xsl"/>
  <xsl:import href="pagesetup.xsl"/>

  <!-- See http://www.sagehill.net/docbookxsl/BordersAndShading.html -->
  <xsl:attribute-set name="admonition.properties">
    <xsl:attribute name="border">0.5pt solid black</xsl:attribute>
    <xsl:attribute name="padding">1pt</xsl:attribute>
    <xsl:attribute name="background-color">#FFE4E1</xsl:attribute>
  </xsl:attribute-set>

  <xsl:attribute-set name="table.table.properties">
    <xsl:attribute name="margin">3pt</xsl:attribute>
    <!--
    <xsl:attribute name="font-size">6pt</xsl:attribute>
    -->
  </xsl:attribute-set>

  <!-- See http://docbook.sourceforge.net/release/xsl-ns/current/doc/html/shade.verbatim.style.html -->
  <xsl:attribute-set name="shade.verbatim.style">
    <xsl:attribute name="border">0.5pt solid gray</xsl:attribute>
    <xsl:attribute name="padding">1pt</xsl:attribute>
    <xsl:attribute name="background-color">#F0F8FF</xsl:attribute>
    <!-- Note that some of OASIS's examples use the long-deprecated "bgcolor".
         You should use "background-color". -->
  </xsl:attribute-set>

  <!-- This allows tables and other block objects to wrap across multiple
       pages -->
  <xsl:attribute-set name="formal.object.properties">
    <xsl:attribute name="keep-together.within-column">auto</xsl:attribute>
  </xsl:attribute-set>

  <!-- This block implements our "improvement", but not absolute fix, for
       truncation of long lines in verbatim blocks.
       We wrap to keep the lines inside the shade boxes, with the limitation
       that tokens without white-space are not broken/wrapped at all.
       You may turn down the font-size to accommodate longer non-whitespace
       tokens.
       -->
  <xsl:attribute-set name="monospace.verbatim.properties"
                     use-attribute-sets="verbatim.properties">
    <xsl:attribute name="wrap-option">wrap</xsl:attribute>
    <!-- By default, long lines in verbatims will overstep the shade box,
         then be truncated -->

    <xsl:attribute name="font-size">8pt</xsl:attribute>
    <!--
         Default is 10pt.
         10pt fits 78 characters inside the shade box, but FO will fold the
         first space character >= 78, leaving a right margin of >= 1 char.
         So with setting of 10 pt, text will only overwrite the shadow box
         for lines containing a non-whitespace-token of length > 78.
    -->
  </xsl:attribute-set>

  <!--
  This suggestion at http://www.dpawson.co.uk/docbook/styling/fo.html#d3043e580
  doesn't work.  It's supposed to insert a space character so that our
  white-space wrapping will force a wrap here regardless of the character at
  that position.
  The other suggestion on the page doesn't work, because it only breaks on
  specific characters.  We want to FORCE a break at column 80, to absolutely
  prevent truncation.
  <xsl:template match="processing-instruction('sbr')">
    <fo:character
      character=" "
      font-size=".01pt"
      treat-as-word-space="true"/>
  </xsl:template>
  -->

  <!-- This would apply to all section levels
  <xsl:attribute-set name="section.title.properties">
    <xsl:attribute name="color">#083194</xsl:attribute>
  </xsl:attribute-set>
  -->
  <xsl:attribute-set name="section.title.level1.properties">
    <xsl:attribute name="color">#000080</xsl:attribute>
    <xsl:attribute name="font-size">
      <xsl:value-of select="$body.font.master * 1.8"></xsl:value-of>
      <xsl:text>pt</xsl:text>
    </xsl:attribute>
  </xsl:attribute-set>
  <xsl:attribute-set name="section.title.level2.properties">
    <xsl:attribute name="font-size">
      <xsl:value-of select="$body.font.master * 1.6"></xsl:value-of>
      <xsl:text>pt</xsl:text>
    </xsl:attribute>
  </xsl:attribute-set>
  <xsl:attribute-set name="section.title.level3.properties">
    <xsl:attribute name="font-size">
      <xsl:value-of select="$body.font.master * 1.4"></xsl:value-of>
      <xsl:text>pt</xsl:text>
    </xsl:attribute>
  </xsl:attribute-set>
  <xsl:attribute-set name="section.title.level4.properties">
    <xsl:attribute name="font-size">
      <xsl:value-of select="$body.font.master * 1.3"></xsl:value-of>
      <xsl:text>pt</xsl:text>
    </xsl:attribute>
  </xsl:attribute-set>
  <xsl:attribute-set name="section.title.level5.properties">
    <xsl:attribute name="font-size">
      <xsl:value-of select="$body.font.master * 1.2"></xsl:value-of>
      <xsl:text>pt</xsl:text>
    </xsl:attribute>
  </xsl:attribute-set>
  <xsl:attribute-set name="section.title.level6.properties">
    <xsl:attribute name="font-size">
      <xsl:value-of select="$body.font.master * 1.1"></xsl:value-of>
      <xsl:text>pt</xsl:text>
    </xsl:attribute>
  </xsl:attribute-set>

  <!-- This sets the size for titles of all all chapters, appendices, articles,
       glossaries, bibliographies, prefaces, indexes, dedications, colophons.
       The much more complex systems documented at
       http://www.sagehill.net/docbookxsl/TitleFontSizes.html don't work for me.
  -->
  <xsl:attribute-set name="component.title.properties">
    <xsl:attribute name="font-size">
      <xsl:value-of select="$body.font.master * 2.1"></xsl:value-of>
      <xsl:text>pt</xsl:text>
    </xsl:attribute>
    <xsl:attribute name="color">#000080</xsl:attribute>
  </xsl:attribute-set>
</xsl:stylesheet> 
