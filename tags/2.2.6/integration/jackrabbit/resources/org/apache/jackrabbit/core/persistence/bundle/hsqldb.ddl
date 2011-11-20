#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#  Bundle persistence DDL script for the HSQLDB database engine (http://www.hsqldb.org)
# 
create table ${schemaObjectPrefix}BUNDLE (NODE_ID binary(16) primary key, BUNDLE_DATA blob(2G) not null))
create table ${schemaObjectPrefix}REFS (NODE_ID binary(16) PRIMARY KEY primary key, REFS_DATA blob(2G) not null))
create table ${schemaObjectPrefix}BINVAL (BINVAL_ID char(64) PRIMARY KEY, BINVAL_DATA blob(2G) not null)
create table ${schemaObjectPrefix}NAMES (ID INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, NAME varchar(255) not null)
