package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * It is a syntactic representation of a derived column with some specific serialization convention corresponding to certain format, for example, programming language or expression language.
 * This class knows about the syntactic conventions of the language/expression language and can also translate it the corresponding object representation.
 * This objects are supposed to be used to represent formulas edited by the user and stored in columns. The system then translates then into an executable form (evaluators).
 * This class has to directly or indirectly encode all names needed for computing certain column kind like main table name, parameter path names, and maybe additional parameters like whether to append records.
 */
public interface ColumnDefinition {
	public ColumnEvaluator translate(); // Transform syntactic representation to object representation by producing the necessary objects like Udes, tables, columns etc.
	public List<DcError> getErrors();
	// String getTable(); TODO: Do we need main table name?
	// List<String> getDependencies(); // TODO: Do we need a list of all columns used in the definition?
}

/**
 * Representation of a calc column using numeric expression libraries like exp4j (library can be chosen as an option).
 * Calc definition is an expression which uses other column paths as parameters. Or it can be a single assignment where lhs is this column. 
 */
class ColumnDefinitionCalc implements ColumnDefinition {
	String formula;
	public String getFormula() {
		return this.formula;
	}
	
	@Override
	public ColumnEvaluator translate() {
		// Generate ColumnEvaluatorCalc object by instantiating the necessary Ude, tables, columns and other objects used during evaluation.
		return null;
	}
	@Override
	public List<DcError> getErrors() {
		return null;
	}

	public ColumnDefinitionCalc(String formula) {
		this.formula = formula;
	}
}

/**
 * Representation of a calc column using numeric expression libraries like exp4j (library can be chosen as an option).
 * Link definition is a collection of assignments represented either syntactically or as an (already parsed) array object. 
 */
class ColumnDefinitionLink implements ColumnDefinition {
	String formula;
	
	@Override
	public ColumnEvaluator translate() {
		// Generate ColumnEvaluatorLink object by instantiating the necessary Ude, tables, columns and other objects used during evaluation.
		return null;
	}
	@Override
	public List<DcError> getErrors() {
		return null;
	}

	DcError linkTranslateStatus;
	// Parse tuple {...} into a list of member assignments and set error
	public Map<String,String> translateLinkFormulas() {
		this.linkTranslateStatus = null;
		if(this.formula == null || this.formula.isEmpty()) return null;

		Map<String,String> mmbrs = new HashMap<String,String>();

		//
		// Check correct enclosure (curly brackets)
		//
		int open = this.formula.indexOf("{");
		int close = this.formula.lastIndexOf("}");

		if(open < 0 || close < 0 || open >= close) {
			this.linkTranslateStatus = new DcError(DcErrorCode.PARSE_ERROR, "Parse error.", "Problem with curly braces. Tuple expression is a list of assignments in curly braces.");
			return null;
		}

		String sequence = this.formula.substring(open+1, close).trim();

		//
		// Build a list of members from comma separated list
		//
		List<String> members = new ArrayList<String>();
		int previousSeparator = -1;
		int level = 0; // Work only on level 0
		for(int i=0; i<sequence.length(); i++) {
			if(sequence.charAt(i) == '{') {
				level++;
			}
			else if(sequence.charAt(i) == '}') {
				level--;
			}
			
			if(level > 0) { // We are in a nested block. More closing parentheses are expected to exit from this block.
				continue;
			}
			else if(level < 0) {
				this.linkTranslateStatus = new DcError(DcErrorCode.PARSE_ERROR, "Parse error.", "Problem with curly braces. Opening and closing curly braces must match.");
				return null;
			}
			
			// Check if it is a member separator
			if(sequence.charAt(i) == ';') {
				members.add(sequence.substring(previousSeparator+1, i));
				previousSeparator = i;
			}
		}
		members.add(sequence.substring(previousSeparator+1, sequence.length()));

		//
		// Create child tuples from members and parse them
		//
		for(String member : members) {
			int eq = member.indexOf("=");
			if(eq < 0) {
				this.linkTranslateStatus = new DcError(DcErrorCode.PARSE_ERROR, "Parse error.", "No equality sign. Tuple expression is a list of assignments.");
				return null;
			}
			String lhs = member.substring(0, eq).trim();
			if(lhs.startsWith("[")) lhs = lhs.substring(1);
			if(lhs.endsWith("]")) lhs = lhs.substring(0,lhs.length()-1);
			String rhs = member.substring(eq+1).trim();

			mmbrs.put(lhs, rhs);
		}

		return mmbrs;
	}

	public ColumnDefinitionLink(String formula) {
		this.formula = formula;
	}
}

/**
 * Representation of a accu column using numeric expression libraries like exp4j (library can be chosen as an option).
 * Link definition is a collection of assignments represented either syntactically or as an (already parsed) array object. 
 */
class ColumnDefinitionAccu implements ColumnDefinition {
	String formula;
	
	@Override
	public ColumnEvaluator translate() {
		// Generate ColumnEvaluatorAccu object by instantiating the necessary Ude, tables, columns and other objects used during evaluation.
		return null;
	}
	@Override
	public List<DcError> getErrors() {
		return null;
	}

	public ColumnDefinitionAccu(String formula) {
		this.formula = formula;
	}
}
