package org.conceptoriented.sc.core;

import java.util.List;
import java.util.Map;

/**
 * Abstract class that can be extended by custom column plug-ins. 
 */
public abstract class EvaluatorBase implements ScEvaluator {

	public Column thisColumn; // It is output column. It is the first column in dependencies.
	@Override
	public void setColumns(List<Column> columns) {
		thisColumn = columns.get(0);
	}

	protected long thisRow;
	@Override
	public void setThisRow(long row) {
		this.thisRow = row;
	}
	
	@Override
	public void beginEvaluate() { }

	@Override
	public void evaluate() { }

	@Override
	public void endEvaluate() {	}

}
