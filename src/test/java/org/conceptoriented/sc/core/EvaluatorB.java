package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.conceptoriented.sc.core.Column;
import org.conceptoriented.sc.core.EvaluatorBase;
import org.conceptoriented.sc.core.Range;

public class EvaluatorB extends EvaluatorBase {

	// We are going to use these columns and want to store direct references to them
	Column columnA;
	
	Range range;

	@Override
	public List<Object> getDependencies() {
		List<Object> deps = new ArrayList<Object>();
		deps.add("A"); // We are going to use column A
		return deps;
	}
	@Override
	public void setColumns(Map<Object,Column> columns) {
		columnA = columns.get("A");
	}

	@Override
	public void beginEvaluate() {
		// Prepare variables
		range = thisColumn.getInput().getRowRange();
		// We also can do some analysis by computing constants
	}
	
	@Override
	public void endEvaluate() {
	}
	
	@Override
	public void evaluate() {
		// Current value can be used for accumulation
		Double currentValue = (Double)thisColumn.getValue(thisRow);
		
		// Previous values can be used for in-column aggregation by the range has to be checked on validity
		if(thisRow-1 >= range.start) {
			Double previousValue = (Double)thisColumn.getValue(thisRow-1);
		}

		Double valueA = (Double)columnA.getValue(thisRow);
		Double result = null;
		if(valueA != null) {
			result = valueA + 2.0;
		}
		
		thisColumn.setValue(thisRow, result);

		// We can also accumulate/update the current value by using SUM
		///thisColumn.setValue(thisRow, currentValue + result);
	}
}
