package org.conceptoriented.sc.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.json.JSONObject;

public class Column {
	Space space;

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
	
	private String evaluatorClass;
	public String getEvaluatorClass() {
		return evaluatorClass;
	}
	public void setEvaluatorClass(String className) {

		//
		// Unload the previous class
		//

		if(evaluatorClass != null) {
			;
		}

		evaluatorClass = className;
		evaluator = null;
		
		//
		// Dynamically load the class by using the space class loader
		//

		ClassLoader classLoader = space.getClassLoader();
		
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
			evaluator = (EvaluatorBase) clazz.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	private EvaluatorBase evaluator;
	public EvaluatorBase getEvaluator() {
		return evaluator;
	}
	public void setEvaluator(EvaluatorBase evaluator) {
		this.evaluator = evaluator;
	}
	public void initEvaluator() {
		if(this.evaluator == null) return;
		
		// Pass direct references to the required columns so that the evaluator can use them during evaluation
		
		// Retrieve dependencies as a list of what this evaluator is going to use
		List<Object> deps = evaluator.getDependencies();
		
		// Resolve all dependencies
		Map<Object, Column> columns = new HashMap<Object, Column>();
		for(Object dep : deps) {
			Column col = space.getColumn(this.getInput().getName(), (String)dep);
			columns.put((String)dep, col);
		}
		
		// Provide the resolved dependencies back to the evaluator
		evaluator.setColumns(columns);
		
		evaluator.thisColumn = this; // Column it belongs to
	}

	/**
	 * Any column has to provide a method which knows how to produce an output value. 
	 * The output is produced by using all other columns.  
	 */
	public void evaluate() {
		// Get dirty offsets
		Range range = input.getNewRange();
		
		// Initialize/prepare evaluator
		// For example, pass direct column references or other info that is needed to access and manipulate data in the space
		this.initEvaluator();
		evaluator.beginEvaluate();

		// Evaluate for all rows in the range
		for(long i=range.start; i<range.end; i++) {
			// Init one iteration
			evaluator.thisRow = i;
			// Really compute
			evaluator.evaluate();
		}

		// De-initialize/clean evaluator. For example, free resources allocated for its computations.
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

		String json = jid + ", " + jname + ", " + jin + ", " + jout;

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

	public Column(Space space, String name, String input, String output) {
		this.space = space;
		this.id = UUID.randomUUID();
		this.name = name;
		this.input = space.getTable(input);
		this.output = space.getTable(output);
		
		values = new Object[1000];
	}
}
