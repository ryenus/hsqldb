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

  
<!--  FOR UNKNOWN REASON, WRAPPING THE IMPORTED template is NOT WORKING!
 <xsl:template name="book.titlepage.recto">
     <xsl:apply-imports/>
 </xsl:template>
 Forced to duplicate it from "titlepage.templates.xsl" in its entirety here.
 -->
<xsl:template name="book.titlepage.recto">
  <xsl:choose>
    <xsl:when test="d:bookinfo/d:title">
      <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:title"/>
    </xsl:when>
    <xsl:when test="d:info/d:title">
      <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:title"/>
    </xsl:when>
    <xsl:when test="d:title">
      <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:title"/>
    </xsl:when>
  </xsl:choose>

  <xsl:choose>
    <xsl:when test="d:bookinfo/d:subtitle">
      <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:subtitle"/>
    </xsl:when>
    <xsl:when test="d:info/d:subtitle">
      <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:subtitle"/>
    </xsl:when>
    <xsl:when test="d:subtitle">
      <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:subtitle"/>
    </xsl:when>
  </xsl:choose>

  <table cellspacing="0" class="titlead"> <tr>
    <td>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:corpauthor"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:corpauthor"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:authorgroup"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:authorgroup"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:author"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:author"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:othercredit"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:othercredit"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:releaseinfo"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:releaseinfo"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:copyright"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:copyright"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:legalnotice"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:legalnotice"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:pubdate"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:pubdate"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:revision"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:revision"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:revhistory"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:revhistory"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:bookinfo/d:abstract"/>
  <xsl:apply-templates mode="book.titlepage.recto.auto.mode" select="d:info/d:abstract"/>
    </td>
    <td class="sponsorad">
      <xi:include href="../doc-src/branding-frag.xhtml"/>
    </td>
  </tr></table>
</xsl:template>/ad-
/
</xsl:stylesheet> 
