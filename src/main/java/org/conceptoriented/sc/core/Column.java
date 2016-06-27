package org.conceptoriented.sc.core;

import java.util.HashMap;
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
	private Table output;
	public Table getOutput() {
		return output;
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
	
	EvaluatorBase evaluator;

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
	public static Column fromJson(String json) {
		JSONObject obj = new JSONObject(json);
		String id = obj.getString("id");
		String name = obj.getString("name");

		JSONObject input_table = obj.getJSONObject("input");
		String input_id = input_table.getString("id");

		JSONObject output_table = obj.getJSONObject("output");
		String output_id = output_table.getString("id");

		return null;
	}
	
	@Override
	public String toString() {
		return "[" + getName() + "]: " + input.getName() + " -> " + output.getName();
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
