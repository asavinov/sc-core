grammar Tuple;
import Common;

@header {
  package org.conceptoriented.sc.core;
}

// tuple is a list of members
tuple
  : '{' member (';' member)* '}'
  ;

// member is an assignment of an arbitrary expression to a name
member
  : name '=' ( tuple | expr )
  ;

expr : .*? ;

name : (ID | DELIMITED_ID) ;
