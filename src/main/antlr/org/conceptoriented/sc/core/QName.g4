grammar QName;
import Common;

@header {
  package org.conceptoriented.sc.core;
}

// path of names
qname
  : name ('.' name)*
  ;

// schema.table.column
qcolumn
  : ((name '.')? name '.')? name
  ;

name : (ID | DELIMITED_ID) ;
