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
		thisColumn = columns.get(0);
		column1 = columns.get(1);
		column2 = columns.get(2);
	}

	@Override
	public void evaluate(long row) {
		double value1 = ((Number)column1.getValue(row)).doubleValue();
		double value2 = ((Number)column2.getValue(row)).doubleValue();

		double result = Double.NaN;
		if(!Double.isNaN(value1) && !Double.isNaN(value2)) {
			result = value1 + value2;
		}
		
		thisColumn.setValue(row, result);
	}

}
