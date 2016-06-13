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
	public Object evaluate() {
		// Get dirty offsets
		Range range = new Range(0,this.length);
		
		// Initialize/prepare evaluator

		// For each dirty offset in the range, evaluate it by executing the corresponding function
		for(long i=range.start; i<range.end; i++) {
			evaluator.thisRow = i;
			evaluator.evaluate();
		}

		// Mark the range as clean

		// De-initialize/clean evaluator

		return null;
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
