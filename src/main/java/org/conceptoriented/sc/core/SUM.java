package org.conceptoriented.sc.core;

import java.util.List;

import org.conceptoriented.sc.core.Column;
import org.conceptoriented.sc.core.EvaluatorBase;

/**
 * Sum of two numeric columns.
 */
public class SUM extends EvaluatorBase {

	Column column1;
	Column column2;
	
	@Override
	public void setColumns(List<Column> columns) {
		column1 = columns.get(0);
		column2 = columns.get(1);
	}

	@Override
	public void evaluate(long row) {
		Object value1 = column1.getValue(row);
		Object value2 = column2.getValue(row);
		
		if(value1 == null) value1 = Double.NaN;
		if(value2 == null) value2 = Double.NaN;

		double result = Double.NaN;
		result = ((Number)value1).doubleValue() + ((Number)value2).doubleValue();
		
		thisColumn.setValue(row, result);
	}

}
