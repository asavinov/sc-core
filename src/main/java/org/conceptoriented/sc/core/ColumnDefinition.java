package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * It is a syntactic representation of a derived column with some specific serialization convention corresponding to certain format, for example, programming language or expression language.
 * This class knows about the syntactic conventions of the language/expression language and can also translate it the corresponding object representation.
 * This objects are supposed to be used to represent formulas edited by the user and stored in columns. The system then translates then into an executable form (evaluators).
 * This class has to directly or indirectly encode all names needed for computing certain column kind like main table name, parameter path names, and maybe additional parameters like whether to append records.
 */
public interface ColumnDefinition {
	// Transform syntactic representation to object representation by producing the necessary internal objects like Udes, tables, columns etc.
	public ColumnEvaluator translate(Column column);
	public List<DcError> getErrors();
	// String getTable(); TODO: Do we need main table name?
	// List<String> getDependencies(); // TODO: Do we need a list of all columns used in the definition?

}


abstract class ColumnDefinitionBase implements ColumnDefinition {

	ColumnDefinitionKind formulaKind;

	List<DcError> errors = new ArrayList<DcError>();
	public boolean hasErrors() {
		if(this.errors.size() == 0) return false;
		else return true;
	}
	@Override
	public List<DcError> getErrors() {
		return this.errors;
	}

	public static String getDescriptorClass(String descriptor) {
		if(descriptor == null) return null;
		JSONObject jdescr = new JSONObject(descriptor);
		return jdescr.getString("class");
	}

	public static List<QName> getDescriptorParameterPaths(String descriptor) {
		List<QName> params = new ArrayList<QName>();
		if(descriptor == null || descriptor.isEmpty()) return params;

		JSONObject jdescr = new JSONObject(descriptor);
		if(jdescr == null || !jdescr.has("parameters")) return params;

		JSONArray jparams = jdescr.getJSONArray("parameters");

		QNameBuilder qnb = new QNameBuilder();
		for (int i = 0 ; i < jparams.length(); i++) {
			QName qn = qnb.buildQName(jparams.getString(i));
			params.add(qn);
		}

		return params;
	}

}

/**
 * Representation of a calc column using numeric expression libraries like exp4j (library can be chosen as an option).
 * Calc definition is an expression which uses other column paths as parameters. Or it can be a single assignment where lhs is this column. 
 */
class ColumnDefinitionCalc extends ColumnDefinitionBase {

	String formula;
	public String getFormula() {
		return this.formula;
	}
	
	@Override
	public ColumnEvaluator translate(Column column) {
		
		Schema schema = column.getSchema();
		Table inputTable = column.getInput();

		UserDefinedExpression expr = null; // We need only one expression

		if(this.formulaKind == ColumnDefinitionKind.EXP4J || this.formulaKind == ColumnDefinitionKind.EVALEX) {
			expr = new UdeJava(this.formula, inputTable);
		}
		else if(this.formulaKind == ColumnDefinitionKind.UDE) {
			// Parse the descriptor by getting JavaClass name and list of parameters
			String className = ColumnDefinitionCalc.getDescriptorClass(this.formula);
			List<QName> params = ColumnDefinitionCalc.getDescriptorParameterPaths(this.formula);
	    	// TODO: Create possible translation errors
			
			// Create instance of Ude class
			ClassLoader classLoader = schema.getClassLoader();
			Class clazz=null;
			try {
				clazz = classLoader.loadClass(className);
		    } catch (ClassNotFoundException e) {
		        e.printStackTrace(); // TODO: Create translation error
		    }
			
			// Create an instance of an expression class
		    try {
		    	expr = (UserDefinedExpression) clazz.newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace(); // TODO: Translation errors
			} catch (IllegalAccessException e) {
				e.printStackTrace(); // TODO: Translation errors
			}
			
			// Bind its parameters to the paths specified in the descriptor
		    expr.setParamPaths(params);
		}
		else {
			; // TODO: Error not implemented
		}
		
		this.errors.addAll(expr.getTranslateErrors());
		if(this.hasErrors()) return null; // Cannot proceed

		ColumnEvaluatorCalc evaluatorCalc = new ColumnEvaluatorCalc(column, expr);

		return evaluatorCalc;
	}

	public ColumnDefinitionCalc(String formula, ColumnDefinitionKind formulaKind) {
		this.formula = formula;
		super.formulaKind = formulaKind;
	}
}

/**
 * Representation of a calc column using numeric expression libraries like exp4j (library can be chosen as an option).
 * Link definition is a collection of assignments represented either syntactically or as an (already parsed) array object. 
 */
class ColumnDefinitionLink extends ColumnDefinitionBase {

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

/**
 * Representation of a accu column using numeric expression libraries like exp4j (library can be chosen as an option).
 * Link definition is a collection of assignments represented either syntactically or as an (already parsed) array object. 
 */
class ColumnDefinitionAccu extends ColumnDefinitionBase {

	String initFormula;
	String accuFormula;
	String finFormula;
	
	String accuTable;
	String accuPath;
	
	@Override
	public ColumnEvaluator translate(Column column) {

		Schema schema = column.getSchema();
		Table inputTable = column.getInput();
		Table outputTable = column.getOutput();

		UserDefinedExpression initExpr = null;
		List<Column> accuPathColumns = null;
		UserDefinedExpression accuExpr = null;
		UserDefinedExpression finExpr = null;

		if(this.formulaKind == ColumnDefinitionKind.EXP4J || this.formulaKind == ColumnDefinitionKind.EVALEX) {
			// Initialization (always initialize - even for empty formula)
			if(this.finFormula == null || this.finFormula.isEmpty()) { // TODO: We need UDE for constants and for equality (equal to the specified column)
				initExpr = new UdeJava(column.getDefaultValue().toString(), inputTable);
			}
			else {
				initExpr = new UdeJava(this.finFormula, inputTable);
			}
			this.errors.addAll(initExpr.getTranslateErrors());
			if(this.hasErrors()) return null; // Cannot proceed

			// Accu table and link (group) path
			Table accuTable = schema.getTable(this.accuTable);
			if(accuTable == null) { // Binding error
				this.errors.add(new DcError(DcErrorCode.BIND_ERROR, "Binding error.", "Cannot find table: " + accuTable));
				return null;
			}
			QName accuLinkPath = QName.parse(this.accuPath);
			accuPathColumns = accuLinkPath.resolveColumns(accuTable);
			if(accuPathColumns == null) { // Binding error
				this.errors.add(new DcError(DcErrorCode.BIND_ERROR, "Binding error.", "Cannot find columns: " + this.accuPath));
				return null;
			}

			// Accumulation
			accuExpr = new UdeJava(this.accuFormula, accuTable);
			this.errors.addAll(accuExpr.getTranslateErrors());
			if(this.hasErrors()) return null; // Cannot proceed

			// Finalization
			if(this.finFormula != null && !this.finFormula.isEmpty()) {
				finExpr = new UdeJava(this.finFormula, inputTable);
				this.errors.addAll(finExpr.getTranslateErrors());
				if(this.hasErrors()) return null; // Cannot proceed
			}
		}
		else if(this.formulaKind == ColumnDefinitionKind.UDE) {
			
		}

		// Use these objects to create an evaluator
		ColumnEvaluatorAccu evaluatorAccu = new ColumnEvaluatorAccu(column, initExpr, accuExpr, finExpr, accuPathColumns);

		return evaluatorAccu;
	}

	public ColumnDefinitionAccu(String initFormula, String accuFormula, String finFormula, String accuTable, String accuPath, ColumnDefinitionKind formulaKind) {
		this.initFormula = initFormula;
		this.accuFormula = accuFormula;
		this.finFormula = finFormula;
		this.accuTable = accuTable;
		this.accuPath = accuPath;

		super.formulaKind = formulaKind;
	}
}



enum ColumnDefinitionKind {
	NONE(0), // No formula. For example, use directly evaluator object 

	AUTO(10), // Auto. Formula kind has to be determined automatically using other parameters. 

	EXP4J(20), // Like "[Column 1] + [Column 2] / 2.0"
	EVALEX(30),

	JAVASCRIPT(40), 

	UDE(50), // For example, "{ class: "com.package.MyUde.class", parameters: [ "Column1", "[Column 2].[Column 3]" ] }"
	;

	private int value;

	public int getValue() {
		return value;
	}

	public static ColumnDefinitionKind fromInt(int value) {
	    for (ColumnDefinitionKind kind : ColumnDefinitionKind.values()) {
	        if (kind.getValue() == value) {
	            return kind;
	        }
	    }
	    return ColumnDefinitionKind.AUTO;
	 }

	private ColumnDefinitionKind(int value) {
		this.value = value;
	}
}
