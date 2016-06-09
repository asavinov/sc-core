package org.conceptoriented.sc;

/**
 * This interface must be implemented by any custom column evaluator (plugin). 
 */
public interface ColumnFunction {

	//
	// Life-cycle methods will be called during initialization only once
	// These methods can be called also in the case of schema/structure change so that this function knows about it. 
	// Also static properties of every instance
	//

	// Get table-column names it depends on
	
	// Set table-column objects it needs (depends on). These objects are supposed to be used for computing the function (without name resolution).

	//
	// Table loop methods will be called once for each record loop
	// It could be useful for initializing internal (static) variables
	// Also, the function could prepare some objects, for example, resolve names or get direct references/pointers to objects which normally may change between evaluation cycles. 
	//
	public void beginEvaluate();
	public void endEvaluate();

	//
	// Value/record methods will be called once for each evaluation 
	//
	public void evaluate();

}
