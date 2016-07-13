package org.conceptoriented.sc.core;

import java.util.List;

import org.conceptoriented.sc.core.Column;
import org.conceptoriented.sc.core.EvaluatorBase;

/**
 * Sum of two numeric columns.
 */
public class EvaluatorSUM extends EvaluatorBase {

	Column column1;
	Column column2;
	
	@Override
	public void setColumns(List<Column> columns) {
		thisColumn = columns.get(0);
		column1 = columns.get(1);
		column2 = columns.get(2);
	}

	@Override
	public void evaluate() {
		Double value1 = (Double)column1.getValue(thisRow);
		Double value2 = (Double)column2.getValue(thisRow);

		Double result = null;
		if(value1 != null && value2 != null) {
			result = value1 + value2;
		}
		
		thisColumn.setValue(thisRow, result);
	}

}
