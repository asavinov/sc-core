package org.conceptoriented.sc;

import java.util.HashMap;
import java.util.Map;

public class Column {
	Space space;

	String name;
	
	Table input;
	Table output;

	EvaluatorBase evaluator;

	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	//
	// Data access
	//
	
	public Object getValue(long row) {
		return null;
	}
	public void setValue(Object value) {
		// TODO Auto-generated method stub
	}
	public long push(Object value) {
		// TODO Auto-generated method stub
		// Append the value and mark as dirty.
		return 0;
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
		Range range = new Range(0,0);
		
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

	public Column(Space space, String name, String input, String output) {
		this.space = space;
		this.name = name;
		this.input = space.getTable(input);
		this.output = space.getTable(output);
	}

}
