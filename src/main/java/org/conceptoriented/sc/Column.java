package org.conceptoriented.sc;

import java.util.HashMap;
import java.util.Map;

public class Column {
	Space space;

	String name;
	public String getName() {
		return name;
	}
	
	Table input;
	Table output;

	EvaluatorBase evaluator;

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
	
	public void setEvaluator(EvaluatorBase evaluator) {
		this.evaluator = evaluator;
		
		Map<Object, Column> columns = new HashMap<Object, Column>();
		columns.put("A", space.getColumn("T", "A"));
		evaluator.setColumns(columns);
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

		// Evaluate for all rows in the range
		for(long i=range.start; i<range.end; i++) {
			// Init one iteration
			evaluator.thisRow = i;
			// Really compute
			evaluator.evaluate();
		}

		// De-initialize/clean evaluator. For example, free resources allocated for its computations.
	}

	@Override
	public String toString() {
		return "[" + getName() + "]: " + input.getName() + " -> " + output.getName();
	}
	
	public Column(Space space, String name, String input, String output) {
		this.space = space;
		this.name = name;
		this.input = space.getTable(input);
		this.output = space.getTable(output);
		
		values = new Object[1000];
	}
}
