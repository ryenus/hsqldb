# SQL2NNN 9075-14 — Part 14: XML-Related Specifications (SQL/XML)
## Annex F (informative) Feature taxonomy
#### Table 16 — Feature taxonomy for optional features

| Feature ID | Feature Name                                                                |
|------------|-----------------------------------------------------------------------------|
| X010       | XML type                                                                    |
| X011       | Arrays of XML type                                                          |
| X012       | Multisets of XML type                                                       |
| X013       | Distinct types of XML type                                                  |
| X014       | Attributes of XML type                                                      |
| X015       | Fields of XML type                                                          |
| X016       | Persistent XML values                                                       |
| X020       | XMLConcat                                                                   |																|
| X025       | XMLCast                                                                     |
| X030       | XMLDocument                                                                 |
| X031       | XMLElement                                                                  |
| X032       | XMLForest                                                                   |
| X034       | XMLAgg                                                                      |
| X035       | XMLAgg: ORDER BY option                                                     |
| X036       | XMLComment                                                                  |
| X037       | XMLPI                                                                       |
| X038       | XMLText                                                                     |
| X040       | Basic table mapping                                                         |
| X041       | Basic table mapping: null absent                                            |
| X042       | Basic table mapping: null as nil                                            |
| X043       | Basic table mapping: table as forest                                        |
| X044       | Basic table mapping: table as element                                       |
| X045       | Basic table mapping: with target namespace                                  |
| X046       | Basic table mapping: data mapping                                           |
| X047       | Basic table mapping: metadata mapping                                       |
| X048       | Basic table mapping: base64 encoding of binary strings                      |
| X049       | Basic table mapping: hex encoding of binary strings                         |
| X050       | Advanced table mapping                                                      |
| X051       | Advanced table mapping: null absent                                         |
| X052       | Advanced table mapping: null as nil                                         |
| X053       | Advanced table mapping: table as forest                                     |
| X054       | Advanced table mapping: table as element                                    |
| X055       | Advanced table mapping: with target namespace                               |
| X056       | Advanced table mapping: data mapping                                        |
| X057       | Advanced table mapping: metadata mapping                                    |
| X058       | Advanced table mapping: base64 encoding of binary strings                   |
| X059       | Advanced table mapping: hex encoding of binary strings                      |
| X060       | XMLParse: Character string input and CONTENT option                         |
| X061       | XMLParse: Character string input and DOCUMENT option                        |
| X065       | XMLParse: BLOB input and CONTENT option                                     |
| X066       | XMLParse: BLOB input and DOCUMENT option                                    |
| X068       | XMLSerialize: BOM                                                           |
| X069       | XMLSerialize: INDENT                                                        |
| X070       | XMLSerialize: Character string serialization and CONTENT option             |
| X071       | XMLSerialize: Character string serialization and DOCUMENT option            |
| X072       | XMLSerialize: Character string serialization                                |
| X073       | XMLSerialize: BLOB serialization and CONTENT option                         |
| X074       | XMLSerialize: BLOB serialization and DOCUMENT option                        |
| X075       | XMLSerialize: BLOB serialization                                            |
| X076       | XMLSerialize: VERSION                                                       |
| X077       | XMLSerialize: explicit ENCODING option                                      |
| X078       | XMLSerialize: explicit XML declaration                                      |
| X080       | Namespaces in XML publishing                                                |
| X081       | Query-level XML namespace declarations                                      |
| X082       | XML namespace declarations in DML                                           |
| X083       | XML namespace declarations in DDL                                           |
| X084       | XML namespace declarations in compound statements                           |
| X085       | Predefined namespace prefixes59 X086 XML namespace declarations in XMLTable |
| X090       | XML document predicate                                                      |
| X091       | XML content predicate                                                       |
| X096       | XMLExists                                                                   |
| X100       | Host language support for XML: CONTENT option                               |
| X101       | Host language support for XML: DOCUMENT option                              |
| X110       | Host language support for XML: VARCHAR mapping                              |
| X111       | Host language support for XML: CLOB mapping                                 |
| X112       | Host language support for XML: BLOB mapping                                 |
| X113       | Host language support for XML: STRIP WHITESPACE option                      |
| X114       | Host language support for XML: PRESERVE WHITESPACE option                   |
| X120       | XML parameters in SQL routines                                              |
| X121       | XML parameters in external routines                                         |
| X131       | Query-level XMLBINARY clause                                                |
| X132       | XMLBINARY clause in DML                                                     |
| X133       | XMLBINARY clause in DDL                                                     |
| X134       | XMLBINARY clause in compound statements                                     |
| X135       | XMLBINARY clause in subqueries                                              |
| X141       | IS VALID predicate: data-driven case                                        |
| X142       | IS VALID predicate: ACCORDING TO clause                                     |
| X143       | IS VALID predicate: ELEMENT clause                                          |
| X144       | IS VALID predicate: schema location                                         |
| X145       | IS VALID predicate outside check constraints                                |
| X151       | IS VALID predicate with DOCUMENT option                                     |
| X152       | IS VALID predicate with CONTENT option                                      |
| X153       | IS VALID predicate with SEQUENCE option                                     |
| X155       | IS VALID predicate: NAMESPACE without ELEMENT clause                        |
| X157       | IS VALID predicate: NO NAMESPACE with ELEMENT clause                        |
| X160       | Basic Information Schema for registered XML Schemas                         |
| X161       | Advanced Information Schema for registered XML Schemas                      |
| X170       | XML null handling options                                                   |
| X171       | NIL ON NO CONTENT option                                                    |
| X181       | XML(DOCUMENT(UNTYPED)) type                                                 |
| X182       | XML(DOCUMENT(ANY)) type                                                     |
| X190       | XML(SEQUENCE) type                                                          |
| X191       | XML(DOCUMENT(XMLSCHEMA)) type                                               |
| X192       | XML(CONTENT(XMLSCHEMA)) type                                                |
| X200       | XMLQuery                                                                    |
| X201       | XMLQuery: RETURNING CONTENT                                                 |
| X202       | XMLQuery: RETURNING SEQUENCE                                                |
| X203       | XMLQuery: passing a context item                                            |
| X204       | XMLQuery: initializing an XQuery variable                                   |
| X205       | XMLQuery: EMPTY ON EMPTY option                                             |
| X206       | XMLQuery: NULL ON EMPTY option                                              |
| X211       | XML 1.1 support                                                             |
| X221       | XML passing mechanism BY VALUE                                              |
| X222       | XML passing mechanism BY REF                                                |
| X231       | XML(CONTENT(UNTYPED)) type                                                  |
| X232       | XML(CONTENT(ANY)) type                                                      |
| X241       | RETURNING CONTENT in XML publishing                                         |
| X242       | RETURNING SEQUENCE in XML publishing                                        |
| X251       | Persistent XML values of XML(DOCUMENT(UNTYPED)) type                        |
| X252       | Persistent XML values of XML(DOCUMENT(ANY)) type                            |
| X253       | Persistent XML values of XML(CONTENT(UNTYPED)) type                         |
| X254       | Persistent XML values of XML(CONTENT(ANY)) type                             |
| X255       | Persistent XML values of XML(SEQUENCE) type                                 |
| X256       | Persistent XML values of XML(DOCUMENT(XMLSCHEMA)) type                      |
| X257       | Persistent XML values of XML(CONTENT(XMLSCHEMA)) type                       |
| X260       | XML type: ELEMENT clause                                                    |
| X261       | XML type: NAMESPACE without ELEMENT clause                                  |
| X263       | XML type: NO NAMESPACE with ELEMENT clause                                  |
| X264       | XML type: schema location                                                   |
| X271       | XMLValidate: data-driven case                                               |
| X272       | XMLValidate: ACCORDING TO clause                                            |
| X273       | XMLValidate: ELEMENT clause                                                 |
| X274       | XMLValidate: schema location                                                |
| X281       | XMLValidate: with DOCUMENT option                                           |
| X282       | XMLValidate with CONTENT option                                             |
| X283       | XMLValidate with SEQUENCE option                                            |
| X284       | XMLValidate NAMESPACE without ELEMENT clause                                |
| X286       | XMLValidate: NO NAMESPACE with ELEMENT clause                               |
| X300       | XMLTable                                                                    |
| X301       | XMLTable: derived column list option                                        |
| X302       | XMLTable: ordinality column option                                          |
| X303       | XMLTable: column default option                                             |
| X304       | XMLTable: passing a context item                                            |
| X305       | XMLTable: initializing an XQuery variable                                   |
| X400       | Name and identifier mapping                                                 |
| X410       | Alter column data type: XML type                                            |
