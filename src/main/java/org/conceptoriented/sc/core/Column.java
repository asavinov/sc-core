package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.common.base.Strings;

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
	public long push(Object value) {
		long row = length++;
		values[(int)row] = value;
		return row;
	}

	public void removeDelRange(Range delRange) {
		// Currently, do not do anything. The deleted values are still there - they are not supposed to be accessed. The table knows about semantics of these intervals.
	}

	//
	// Evaluate and formula
	//
	
	private List<List<Integer>> getPathEntries(String expr) { // Find all entries of column paths and return pairs start-end
		List<List<Integer>> names = new ArrayList<List<Integer>>();
		
		if(expr == null || expr.isEmpty()) return names;
		
		String ex =  "\\[(.*?)\\]";
		String ex2 = "[\\[\\]]";

		Pattern p = Pattern.compile(ex,Pattern.DOTALL);
		Matcher matcher = p.matcher(expr);
		while(matcher.find())
		{
			int s = matcher.start();
			int e = matcher.end();
			String name = matcher.group();
			List<Integer> entry = new ArrayList<Integer>();
			entry.add(s);
			entry.add(e);
			names.add(entry);
		}
		
		// If between two names there is only dot then combine them
		List<List<Integer>> paths = new ArrayList<List<Integer>>();
		for(int i = 0; i < names.size()-1; i++) {
			int thisEnd = names.get(i).get(1);
			int nextStart = names.get(i+1).get(0);
			
			if(expr.substring(thisEnd, nextStart).trim() == ".") { // There is continuation.
				names.get(i+1).set(0, names.get(i).get(0)); // Attach this name to the next name as a prefix
			}
			else { // No continuation. Ready to copy as path.
				paths.add(names.get(i));
			}
		}
		
		return paths;
	}

	private String descriptor;
	public String getDescriptor() {
		return descriptor;
	}
	public String getEvaluatorClass() {
		if(descriptor == null) return null;
		JSONObject jdescr = new JSONObject(descriptor);
		return jdescr.getString("class");
	}
	public List<QName> getDependencies() {
		List<QName> deps = new ArrayList<QName>();
		if(descriptor == null || descriptor.isEmpty()) return deps;

		JSONObject jdescr = new JSONObject(descriptor);
		if(jdescr == null || !jdescr.has("dependencies")) return deps;

		JSONArray jdeps = jdescr.getJSONArray("dependencies");

		QNameBuilder qnb = new QNameBuilder();
		for (int i = 0 ; i < jdeps.length(); i++) {
			QName qn = qnb.buildQName(jdeps.getString(i));
			deps.add(qn);
		}

		return deps;
	}
	public void setDescriptor(String descriptor) {
		this.descriptor = descriptor;

		//
		// Resolve all dependencies declared in the descriptor (the first column in the dependencies must be this/output column)
		//
		List<Column> columns = new ArrayList<Column>();
		for(QName dep : this.getDependencies()) {
			Column col = dep.resolveColumn(schema, this.getInput());
			columns.add(col);
		}
		
		// Update dependency graph
		schema.setDependency(this, columns);

		// Here we might want to check the validity of the dependency graph (cycles, at least for this column)
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

	/**
	 * Any column has to provide a method which knows how to produce an output value. 
	 * The output is produced by using all other columns.  
	 */
	public void evaluate() {
		this.begingEvaluate(); // Prepare (evaluator, computational resources etc.)
		
		if(evaluator == null) return;

		// Evaluate for all rows in the (dirty, new) range
		Range range = input.getNewRange();
		for(long i=range.start; i<range.end; i++) {
			evaluator.evaluate(i);
		}

		this.endEvaluate(); // De-initialize (evaluator, computational resources etc.)
	}

	protected void endEvaluate() {
		evaluator.endEvaluate();
	}

	public String toJson() {
		// Trick to avoid backslashing double quotes: use backticks and then replace it at the end 
		String jid = "`id`: `" + this.getId() + "`";
		String jname = "`name`: `" + this.getName() + "`";
		
		String jinid = "`id`: `" + this.getInput().getId() + "`";
		String jin = "`input`: {" + jinid + "}";

		String joutid = "`id`: `" + this.getOutput().getId() + "`";
		String jout = "`output`: {" + joutid + "}";

		//String jdescr = "`descriptor`: " + (this.getDescriptor() != null ? "`"+this.getDescriptor()+"`" : "null");
		String jdescr = "`descriptor`: " + JSONObject.valueToString(this.getDescriptor()) + "";

		String json = jid + ", " + jname + ", " + jin + ", " + jout + ", " + jdescr;

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
