package org.conceptoriented.sc.core;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.json.JSONArray;
import org.json.JSONObject;

import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import net.objecthunter.exp4j.ValidationResult;

public class Column {
	private Schema schema;
	public Schema getSchema() {
		return schema;
	}
	
	private final UUID id;
	public UUID getId() {
		return id;
	}

	private String name;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	private Table input;
	public Table getInput() {
		return input;
	}
	public void setInput(Table table) {
		this.input = table;
	}

	private Table output;
	public Table getOutput() {
		return output;
	}
	public void setOutput(Table table) {
		this.output = table;
	}

	//
	// Data access
	//
	
	Object[] values;
	long length = 0;
	
	public Object getValue(long row) {
		return values[(int)row];
	}
	public void setValue(long row, Object value) {
		values[(int)row] = value;
	}
	public long appendValue(Object value) {
		// Cast the value to type of this column
		if(this.getOutput().getName().equalsIgnoreCase("String")) {
			value = value.toString();
		}
		else if(this.getOutput().getName().equalsIgnoreCase("Double") || this.getOutput().getName().equalsIgnoreCase("Integer")) {
			if(value instanceof String) {
				try { value = NumberFormat.getInstance(Locale.US).parse((String)value); } 
				catch (ParseException e) { value = Double.NaN; }
			}
		}
		
		long row = length++;
		values[(int)row] = value;
		return row;
	}

	public void removeDelRange(Range delRange) {
		// Currently, do not do anything. The deleted values are still there - they are not supposed to be accessed. The table knows about semantics of these intervals.
	}

	// Convenience method. The first element should be this column. 
	protected Object getValue(List<Column> columns, long row) {
		Object out = row;
		for(Column col : columns) {
			out = col.getValue((long)out);
			if(out == null) break;
		}
		return out;
	}

	//
	// Formula
	//
	
	protected String formula;
	public String getFormula() {
		return formula;
	}
	public void setFormula(String formula) {
		this.formula = formula;
		translate(); // Do it after each assignment in order to get status
	}
	
	protected String facttable;
	public String getFacttable() {
		return facttable;
	}
	public void setFacttable(String facttable) {
		this.facttable = facttable;
	}
	
	protected String grouppath;
	public String getGrouppath() {
		return grouppath;
	}
	public void setGrouppath(String grouppath) {
		this.grouppath = grouppath;
	}
	
	/**
	 * Status of the column. Currently, it is own status of the formula (no propagation) and it is only translation (parse and bind) status (not evaluation).
	 */
	public DcError getStatus() {
		if(expression == null) {
			return new DcError(DcErrorCode.NONE, "", "");
		}
		else {
			ExprNode errorNode = expression.getErrorNode();
			if(errorNode == null) {
				return new DcError(DcErrorCode.NONE, "", "");
			}
			else {
				return errorNode.status;
			}
		}
	}

	//
	// Translate
	//
	
	public ExprNode expression;
	
	public void translate() {
		if(formula == null || formula.isEmpty()) {
			this.expression = null;
			schema.setDependency(this, null); // Non-evaluatable column independent of the reason
			return;
		}
		else {
			List<Column> columns = new ArrayList<Column>();

			// Parse (check correct syntax of strings only)
			expression = new ExprNode();
			expression.formula = this.formula;
			expression.name = this.name;
			expression.facttableName = this.facttable;
			expression.grouppathName = this.grouppath;
			
			expression.parse();

			// Bind (check if all the symbols can be resolved)
			expression.table = this.getInput(); // It will be passed recursively to all child expressions
			expression.column = this;

			expression.bind();

			// TODO: What kind of dependencies we need in the case of aggregation?
			
			// Reset column data before evaluation. 0.0 for numeric. In future, additional param in formula can be used (for grouping) as well as postprocessing value like normal formula. So formula can be always thought of as initializor. In addition, we can add a collecotr formula for aggregation. And finally, we can add a finalizer executed after aggregation as a normal formula.
			// Check that evaluation should not be called for formula/column updates (only explicitly).
			// TODO: visualize none/group/tuple/calc columns differently (method getFormulaType similar to getStatus). It could be an icon.
			// Check consistency of formula types: primitive -> only group and calc, non-primitive -> only tuple. Initially, as error message. Later, hiding irrelevant UI controls.
			// Maybe introduce an explicit selector "formula type"=none_or_external/calc/tuple/group. It will be stored in a user provided property (what the user wants).
			
			columns = expression.getDependencies();
			schema.setDependency(this, columns); // Update dependency graph
		}
	}
		
	//
	// Evaluate
	//

	public void evaluate() {
		
		if(formula == null || formula.isEmpty()) return;
		
		//
		// Translate
		//
		translate();

		//
		// Evaluate
		//

		// TODO: Reset current column (important for grouping)
		expression.beginEvaluate();

		if(!expression.isAggregated()) {
			Table looptable = input;
			Range range = looptable.getNewRange(); // All dirty/new rows
			for(long i=range.start; i<range.end; i++) {

				expression.evaluate(i);

				this.setValue(i, expression.result); // Store the output value for the current row
			}
		}
		else {
			Table looptable = expression.facttable;
			Range range = looptable.getNewRange(); // All dirty/new rows

			for(long i=range.start; i<range.end; i++) {
				long g = (Long) expression.grouppath.get(0).getValue(expression.grouppath, i); // Find group element
				//Object output = this.getValue(g); // Read current output

				expression.evaluate(i);

				this.setValue(g, expression.result); // Store the output value for the current row
			}
		}

	}

	//
	// Descriptor
	//
	
	private String descriptor;
	public String getDescriptor() {
		return descriptor;
	}
	public void setDescriptor(String descriptor) {
		this.descriptor = descriptor;
		
		if(this.formula != null && !this.formula.isEmpty()) {
			return; // If there is formula then descriptor is not used for dependencies
		}

		//
		// Resolve all dependencies
		//
		List<Column> columns = new ArrayList<Column>();

		if(descriptor != null && !descriptor.isEmpty()) {

			columns = getEvaluatorDependencies();

			schema.setDependency(this, columns); // Update dependency graph
			return;
		}
		else {
			schema.setDependency(this, null); // Non-evaluatable column for any reason
		}

		// Here we might want to check the validity of the dependency graph (cycles, at least for this column)
	}

	public List<Column> getEvaluatorDependencies() {
		List<Column> columns = new ArrayList<Column>();

		List<QName> deps = new ArrayList<QName>();
		if(descriptor == null || descriptor.isEmpty()) return columns;

		JSONObject jdescr = new JSONObject(descriptor);
		if(jdescr == null || !jdescr.has("dependencies")) return columns;

		JSONArray jdeps = jdescr.getJSONArray("dependencies");

		QNameBuilder qnb = new QNameBuilder();
		for (int i = 0 ; i < jdeps.length(); i++) {
			QName qn = qnb.buildQName(jdeps.getString(i));
			deps.add(qn);
		}

		for(QName dep : deps) {
			Column col = dep.resolveColumn(schema, this.getInput());
			columns.add(col);
		}
		
		return columns;
	}
	public String getEvaluatorClass() {
		if(descriptor == null) return null;
		JSONObject jdescr = new JSONObject(descriptor);
		return jdescr.getString("class");
	}

	protected ScEvaluator evaluator;
	public ScEvaluator setEvaluator() {
		evaluator = null;
		
		String evaluatorClass = getEvaluatorClass(); // Read from the descriptor
		if(evaluatorClass == null) return null;
		
		//
		// Dynamically load the class by using the schema class loader
		//

		ClassLoader classLoader = schema.getClassLoader();
		
		Class clazz=null;
		try {
			clazz = classLoader.loadClass(evaluatorClass);
	    } catch (ClassNotFoundException e) {
	        e.printStackTrace();
	    }
		
		//
		// Create an instance of an evaluator
		//
	    try {
			evaluator = (ScEvaluator) clazz.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		
		return evaluator;
	}

	protected void begingEvaluate() {
		//
		// Prepare evaluator instance
		//

		if(evaluator == null) {
			setEvaluator();
		}
		
		if(evaluator == null) return;
		
		// Pass direct references to the required columns so that the evaluator can use them during evaluation. The first element has to be this (output) column
		evaluator.setColumn(this);
		List<Column> columns = schema.getDependency(this);
		evaluator.setColumns(columns);
		
		evaluator.beginEvaluate();
	}

	protected void endEvaluate() {
		evaluator.endEvaluate();
	}

	/**
	 * Evaluate class. 
	 */
	public void evaluateDescriptor() {
		
		if(descriptor == null || descriptor.isEmpty()) return; 
			
		this.begingEvaluate(); // Prepare (evaluator, computational resources etc.)
		
		if(evaluator == null) return;

		// Evaluate for all rows in the (dirty, new) range
		Range range = input.getNewRange();
		for(long i=range.start; i<range.end; i++) {
			evaluator.evaluate(i);
		}

		this.endEvaluate(); // De-initialize (evaluator, computational resources etc.)
	}

	public String toJson() {
		// Trick to avoid backslashing double quotes: use backticks and then replace it at the end 
		String jid = "`id`: `" + this.getId() + "`";
		String jname = "`name`: `" + this.getName() + "`";
		
		String jinid = "`id`: `" + this.getInput().getId() + "`";
		String jin = "`input`: {" + jinid + "}";

		String joutid = "`id`: `" + this.getOutput().getId() + "`";
		String jout = "`output`: {" + joutid + "}";

		String jfmla = "`formula`: " + JSONObject.valueToString(this.getFormula()) + "";
		String jftbl = "`facttable`: " + JSONObject.valueToString(this.getFacttable()) + "";
		String jgrp = "`grouppath`: " + JSONObject.valueToString(this.getGrouppath()) + "";

		//String jdescr = "`descriptor`: " + (this.getDescriptor() != null ? "`"+this.getDescriptor()+"`" : "null");
		String jdescr = "`descriptor`: " + JSONObject.valueToString(this.getDescriptor()) + "";

		String jstatus = "`status`: " + (this.getStatus() != null ? this.getStatus().toJson() : "undefined");

		String json = jid + ", " + jname + ", " + jin + ", " + jout + ", " + jfmla + ", " + jftbl + ", " + jgrp + ", " + jdescr + ", " + jstatus;

		return ("{" + json + "}").replace('`', '"');
	}
	
	@Override
	public String toString() {
		return "[" + getName() + "]: " + input.getName() + " -> " + output.getName();
	}
	
	@Override
	public boolean equals(Object aThat) {
		if (this == aThat) return true;
		if ( !(aThat instanceof Table) ) return false;
		
		Column that = (Column)aThat;
		
		if(!that.getId().toString().equals(id.toString())) return false;
		
		return true;
	}

	public Column(Schema schema, String name, String input, String output) {
		this.schema = schema;
		this.id = UUID.randomUUID();
		this.name = name;
		this.input = schema.getTable(input);
		this.output = schema.getTable(output);
		
		values = new Object[1000];
	}
}
