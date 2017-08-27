package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Representation of a calc column using numeric expression libraries like exp4j (library can be chosen as an option).
 * Link definition is a collection of assignments represented either syntactically or as an (already parsed) array object. 
 */
public class ColumnDefinitionLink extends ColumnDefinitionBase {

	String formula;
	public String getFormula() {
		return this.formula;
	}
	
	@Override
	public ColumnEvaluator translate(Column column) {

		Schema schema = column.getSchema();
		Table inputTable = column.getInput();
		Table outputTable = column.getOutput();

		List<Pair<Column,UserDefinedExpression>> exprs = new ArrayList<Pair<Column,UserDefinedExpression>>();

		if(this.formulaKind == ColumnDefinitionKind.EXP4J || this.formulaKind == ColumnDefinitionKind.EVALEX) {
			// Parse tuple and create a collection of assignments
			Map<String,String> mmbrs = this.translateLinkFormulas();
			if(this.hasErrors()) return null; // Cannot proceed

			// Create column-expression pairs for each assignment
			for(Entry<String,String> mmbr : mmbrs.entrySet()) { // For each tuple member (assignment) create an expression

				// Right hand side
				UdeJava expr = new UdeJava(mmbr.getValue(), inputTable);
				
				this.errors.addAll(expr.getTranslateErrors());
				if(this.hasErrors()) return null; // Cannot proceed

				// Left hand side (column of the type table)
				Column assignColumn = schema.getColumn(outputTable.getName(), mmbr.getKey());
				if(assignColumn == null) { // Binding error
					this.errors.add(new DcError(DcErrorCode.BIND_ERROR, "Binding error.", "Cannot find column: " + assignColumn));
					return null;
				}

				exprs.add(Pair.of(assignColumn, expr));
			}
		}
		else if(this.formulaKind == ColumnDefinitionKind.UDE) {
			// TODO: List of pairs: "[ { "column": "Col1", "descriptor": { "class": "com.class", "parameters": ["p1","p2"] }  }, {}, ...  ]"
			// Here more useful would be equality descriptor, e.g., mapping { "MyCol1":"Path1", "MyCol2":"Path2" }
			// Or as pairs: { "column":"Col1", "expression":"Path1" }, ...
			// It is similar to having a formula but we use special class of Ude without numeric expressions and fast direct access to certain column or path
		}

		// Use this list of assignments to create an evaluator
		ColumnEvaluatorLink evaluatorLink = new ColumnEvaluatorLink(column, exprs);

		return evaluatorLink;
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

	public ColumnDefinitionLink(String formula, ColumnDefinitionKind formulaKind) {
		this.formula = formula;
		super.formulaKind = formulaKind;
	}
}