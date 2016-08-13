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

		//
		// Resolve all dependencies
		//
		List<Column> columns = new ArrayList<Column>();
		if(formula != null && !formula.isEmpty()) {
			columns = getComputeDependencies();
		}
		else if(descriptor != null && !descriptor.isEmpty()) {
			columns = getEvaluatorDependencies();
		}

		schema.setDependency(this, columns); // Update dependency graph

		// Here we might want to check the validity of the dependency graph (cycles, at least for this column)
	}

	//
	// Compute formula
	//

	protected List<ComputeFormulaDependency> computeDependencies = new ArrayList<ComputeFormulaDependency>();
	// Find all entries of column paths. Store the result in the field and return only column list. 
	public List<Column> getComputeDependencies() {
		List<Column> columns = new ArrayList<Column>();

		String exprString = formula;
		
		if(exprString == null || exprString.isEmpty()) {
			computeDependencies = new ArrayList<ComputeFormulaDependency>();
			return columns;
		}
		
		String ex =  "\\[(.*?)\\]";
		//String ex = "[\\[\\]]";
		Pattern p = Pattern.compile(ex,Pattern.DOTALL);
		Matcher matcher = p.matcher(exprString);

		List<ComputeFormulaDependency> names = new ArrayList<ComputeFormulaDependency>();
		while(matcher.find())
		{
			int s = matcher.start();
			int e = matcher.end();
			String name = matcher.group();
			ComputeFormulaDependency entry = new ComputeFormulaDependency();
			entry.start = s;
			entry.end = e;
			names.add(entry);
		}
		
		//
		// If between two names there is only dot then combine them
		//
		List<ComputeFormulaDependency> paths = new ArrayList<ComputeFormulaDependency>();
		for(int i = 0; i < names.size(); i++) {
			if(i == names.size()-1) { // Last element does not have continuation
				paths.add(names.get(i));
				break;
			}
			
			int thisEnd = names.get(i).end;
			int nextStart = names.get(i+1).start;
			
			if(exprString.substring(thisEnd, nextStart).trim() == ".") { // There is continuation.
				names.get(i+1).start = names.get(i).start; // Attach this name to the next name as a prefix
			}
			else { // No continuation. Ready to copy as path.
				paths.add(names.get(i));
			}
		}
		
		//
		// Resolve each column name in the path
		//
		Table table = this.getInput();
    	QNameBuilder parser = new QNameBuilder();
    	
		for(ComputeFormulaDependency dep : paths) {
			dep.pathName = formula.substring(dep.start, dep.end);
			dep.qname = parser.buildQName(dep.pathName);
			dep.columns = dep.qname.resolveColumns(table);
		}

		computeDependencies = paths;

		//
		// Prepare list of columns for return
		//
		for(ComputeFormulaDependency dep : computeDependencies) { // Each dependency is a path which can included repeated segments
			for(Column col : dep.columns) {
				if(!columns.contains(col)) 
					columns.add(col);
			}
		}
		
		return columns;
	}
	

	protected String transformedComputeFormula;
	protected Expression computeExpression;

	public void buildComputeExpression() {
		String exprString = formula;

		//
		// Transform the expression by using new names and get an executable expression
		//
		StringBuffer buf = new StringBuffer(exprString);
		for(int i = computeDependencies.size()-1; i >= 0; i--) {
			ComputeFormulaDependency dep = computeDependencies.get(i);
			dep.paramName = "__p__"+i;
			buf.replace(dep.start, dep.end, dep.paramName);
		}
		
		transformedComputeFormula = buf.toString();

		//
		// Create expression object with the transformed formula
		//
		ExpressionBuilder builder = new ExpressionBuilder(transformedComputeFormula);
		Set<String> vars = computeDependencies.stream().map(x -> x.paramName).collect(Collectors.toCollection(HashSet::new));
		builder.variables(vars);
		Expression exp = builder.build();
		
		ValidationResult res = exp.validate(); // Boolean argument can be used to ignore unknown variables
		res.isValid();
		
		computeExpression = exp;
	}

	public void evaluateComputeExpression() {
		
		// Evaluate for all rows in the (dirty, new) range
		Range range = input.getNewRange();
		for(long i=range.start; i<range.end; i++) {

			// For each input, read all necessary column values
			for(ComputeFormulaDependency dep : computeDependencies) {
				Object value = this.getValue(dep.columns, i);
				if(value == null) {
					value = Double.NaN;
				}
				computeExpression.setVariable(dep.paramName, ((Number)value).doubleValue()); // Pass these values into the expression
			}

			// Evaluate and get output value
			Double result = computeExpression.evaluate();

			// Store the output value for the current row
			this.setValue(i, result);
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

		List<Column> columns = new ArrayList<Column>();
		if(formula != null && !formula.isEmpty()) {
			columns = getComputeDependencies();
		}
		else if(descriptor != null && !descriptor.isEmpty()) {
			columns = getEvaluatorDependencies();
		}

		schema.setDependency(this, columns); // Update dependency graph

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
	 * Any column has to provide a method which knows how to produce an output value. 
	 */
	public void evaluate() {
		
		if(formula != null && !formula.isEmpty()) { // Use formula
	        this.getComputeDependencies();
	        this.buildComputeExpression();
	        this.evaluateComputeExpression();
		}
		else if(descriptor != null && !descriptor.isEmpty()) { // Use descriptor
			
			this.begingEvaluate(); // Prepare (evaluator, computational resources etc.)
			
			if(evaluator == null) return;

			// Evaluate for all rows in the (dirty, new) range
			Range range = input.getNewRange();
			for(long i=range.start; i<range.end; i++) {
				evaluator.evaluate(i);
			}

			this.endEvaluate(); // De-initialize (evaluator, computational resources etc.)
		}
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

		//String jdescr = "`descriptor`: " + (this.getDescriptor() != null ? "`"+this.getDescriptor()+"`" : "null");
		String jdescr = "`descriptor`: " + JSONObject.valueToString(this.getDescriptor()) + "";

		String json = jid + ", " + jname + ", " + jin + ", " + jout + ", " + jfmla + ", " + jdescr;

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

class ComputeFormulaDependency {
	public int start;
	public int end;
	public String pathName;
	public String paramName;
	public QName qname;
	public List<Column> columns;
}
