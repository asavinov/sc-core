package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * It is actually a member (assignment) rather than a tuple (a combination of members). 
 */
public class Tuple {
	
	public String name;

	public String expression; // If a value is an expression (including constants)
	public List<Tuple> members = new ArrayList<Tuple>(); // If value is a combination of expressions

	public boolean isTerminal() {
		// Whether we can continue tuple tree, that is, this node can be expanded
		if(members == null || members.size() == 0) return true;
		else return false;
	}
	
	// Return a tree of tuples
	public static Tuple parseAssignment(String assignment) {
		if(assignment == null || assignment.isEmpty()) return null;
		
		Tuple t = new Tuple();

		// Any assignment has well defined structure QName=( {sequence of assignments} | expression) 
		int eq = assignment.indexOf("=");
		
		if(eq < 0) return null; // Syntax error

		// Extract name
		String name = assignment.substring(0, eq).trim();
		if(name.startsWith("[")) name = name.substring(1);
		if(name.endsWith("]")) name = name.substring(0,name.length()-1);
		t.name = name;

		// Extract value
		String value = assignment.substring(eq+1).trim();
		int open = value.indexOf("{");
		int close = value.lastIndexOf("}");
		
		if(open < 0 && close < 0) { // Primitive value (expression)
			t.expression = value;
			return t;
		}
		else if(open >= 0 && close >= 0 && open < close) { // Tuple - combination of assignments
			String sequence = value.substring(open+1, close-1);
			String[] members = sequence.split(";");
			for(String member : members) {
				Tuple memberAssignment = parseAssignment(member);
				t.members.add(memberAssignment);
			}
			return t;
		}
		else {
			return null; // Syntax error
		}
	}

}
