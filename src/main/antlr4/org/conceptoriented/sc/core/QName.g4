grammar QName;
import Common;

// path of names
qname
  : name ('.' name)*
  ;

// schema.table.column
qcolumn
  : ((name '.')? name '.')? name
  ;

name : (ID | DELIMITED_ID) ;
