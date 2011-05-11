$Id$

BUILDING

You need JDK 1.5 or later and Ant 1.7 or later.

Run "ant gen-docs" from the build subdirectory.
Error messages should be self-explanatory.


SYSTEM

See http://pub.admc.com/howtos/ant-docbook-howto/system-chapt.html#system-features-sect and
http://pub.admc.com/howtos/ant-docbook-howto/tips-app.html for
important tips.  This HOWTO document explains the build system used here.


EDITING

Use DocBook v. 5 syntax in your DocBook source XML files.

Because of the amount of extra infrastructure needed for DocBook olinking,
we are not supporting direct inter-document linking.  If you want to refer
from one DocBook document to content in another one, then add a link to
the canonical document (like using a &distro_bseurl;/guide/index.html link)
and just describe the location referred to.

Top-level DocBook source files for individual DocBook documents reside at
doc-src/X/X.xml.  Other files may be xincluded into these files, and lots of
other resources are referenced or pulled in from under doc-src.

Please use the product name HyperSQL in major titles.  Where introducing
HyperSQL, use a subtitle like "aka HSQLDB".
In the text, use "HyperSQL" as the product name.
In filepaths and package names you code as required for the filepath or package
name, of course.

Don't capitalize words or phrases to emphasize them (including in
section titles or headings).
If you want to emphasize something a certain way, then use a DocBook emphasis
role, and leave the presentation decisions to the style sheets.
It is very easy to set a CSS style to capitalize headings if you want them to
appear that way.
