<?xml version='1.0'?>
<!-- $Id$ -->
<!-- Contents of this file apply to both regular HTML and Chunk HTML formats -->
<!-- See http://www.sagehill.net/docbookxsl/CustomDb5Xsl.html for general
     syntax. -->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
     xmlns:xi="http://www.w3.org/2001/XInclude"
     xmlns:d="http://docbook.org/ns/docbook" exclude-result-prefixes="d">
  <!-- See http://www.sagehill.net/docbookxsl/HTMLHeaders.html -->

  <xsl:template name="user.footer.content">
    <HR/>
    <P class="svnrev">
      <xsl:value-of select="/*/d:info/d:releaseinfo"/>
    </P>
    <xsl:apply-templates select="/*/d:info/d:copyright"
                         mode="titlepage.mode"/>
  </xsl:template>

  <!-- Nesting example:
  <xsl:template name="user.header.content">
    <xsl:call-template name="breadcrumbs"/>
  </xsl:template>
  -->

</xsl:stylesheet> 
