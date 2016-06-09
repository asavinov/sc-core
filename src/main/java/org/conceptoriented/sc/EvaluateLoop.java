package org.conceptoriented.sc;

/**
 * This class is responsible for periodic updating the data state.
 * 
 * @author savinov
 */
public class EvaluateLoop {


	/**
	 * This method implements one period in data processing.
	 * It is normally called if the state is made dirty as a result of adding new data (or for any other reason like manually marking data dirty).
	 * Its task is to bring the state back to consistent state by re-computing the values which are known to be dirty.  
	 */
	public void evaluateSpace() {
		
		// For each table in the space, evaluate it
		// Use the sequence the tables have been created
		// In future, we need to use more complex dependencies (among columns)
		
	}

	public void evaluateTable() {
		
		// For each column in the table, evaluate it
		// Use the sequence the columns have been created
		
	}

	public void evaluateColumn() {
		
		// For each dirty value, evaluate it again and store the result
		// Any column has to provide an evaluation function which knows how to compute the output value.
		
		Column col = null;
		long dirtyRow = 0;
		col.eval(dirtyRow);
		
	}

}
