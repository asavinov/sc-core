package org.conceptoriented.sc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EvaluatorB extends EvaluatorBase {

	// We are going to use these columns and want to store direct references to them
	Column columnA;

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

	public void evaluate() {
		double currentValue = (double)thisColumn.getValue(thisRow);

		double result = (double)columnA.getValue(thisRow) + 2.0;
		
		thisColumn.setValue(result);

		// We could also accumulate/update the current value by using SUM
		thisColumn.setValue(currentValue + result);
	}
}
