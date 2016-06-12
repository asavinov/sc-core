package org.conceptoriented.sc;

import java.util.List;
import java.util.Map;

/**
 * Abstract class that can be extended by custom column plug-ins. 
 */
public abstract class EvaluatorBase implements ScEvaluator {

	//
	// It will be initialized in the constructor (or after some significant changes)
	//
	public Column thisColumn;
	
	//
	// Life-cycle methods will be called during initialization only once
	// These methods can be called also in the case of schema/structure change so that this function knows about it. 
	// Also static properties of every instance
	//

	// Get table-column names it depends on. They can be derived from column dependencies.
	
	// Set table-column objects it needs (depends on). 
	// These objects are supposed to be used for computing the function (without name resolution).
	// We assume that it is static information in the same sense as schema is static and any column is designed to use fixed other columns.
	public List<Object> getDependencies() {
		// This method will be called from outside because the system must build execution/evaluation plan
		// It can be called at these moments:
		// - instantiation of the evaluator
		// - change of the space structure or whenever the system thinks an update is needed
		// It is a run time approach where dependencies are encoded in the evaluator plug-in. 
		// In the case of no plug-in, for instance, if functionality is provided by a structure like COEL or mapping, 
		// dependencies are also provided in the column function definition as configuration. 
		// For example, column definition could have a field with dependencies similar to the name field
		// For example: name:"Amount", dependencies:"[bla-bla-json]"
		// Also, it might be possible to extract dependencies from the COEL formula.
		// If plug-ins have configuration info then dependencies could be included into it along with other parameters. 
		// For example, the functionality of a generic plug-in could require different columns depending on other parameters.
		// Manually configuration in this case can override this run time dependency information. 
		
		// We need to return some standard dependency structure. 
		// It is a list of entries
		// Each entry is a fully-qualified name of a column. We need a special object FQCN like Space:Table:Column.
		
		// Note that it is a static function because dependencies are constant for the whole class so it is characteristic of each class and not instance.

		return null;
	}

	public void setColumns(Map<Object,Column> columns) {
		// The system will pass direct references to all the necessary column objects.
		// This method can be called at any time but we can assume that it will be called when a column is created and its function plug-in instantiated.
		// These columns object have to be stored so that they can be used in the evaluation method to access data in these columns.
	}

	//
	// Table loop methods will be called once for each record loop
	// It could be useful for initializing internal (static) variables
	// Also, the function could prepare some objects, for example, resolve names or get direct references/pointers to objects which normally may change between evaluation cycles. 
	//
	public void beginEvaluate() {
		// It is called just before the evaluation loop, that is, before evaluate methods multiple times executes for the same table
		
		// Here we have access to new/old intervals, column data objects
		// We do not have access to 'this' 
	}
	public void endEvaluate() {
		
	}

	//
	// Initialize and result 
	//
	public long thisRow; // It will be set by the loop driver 

	//
	// Value/record methods will be called once for each evaluation 
	//
	public void evaluate() {
		// It will be called from each (dirty) input of the table
		// Here we need to access:
		// 'this' value (long)
		// column data object reference for all columns we need and declared: object ref
		//   these object references have data access API: column.getValue(input) where input can be 'this' or output of other getValue
		//   this includes this column data object so that we can access our own column values: thisColumn.getValue(offset)
		// output/type table is needed if we want to push records into output
		// input table might be needed just to know more about this column
		
		// So the main object we want to use is a column reference which provides access to data
		// How we use it? And how we reference these column objects from the evaluation method? Indeed, there are many columns we want to use. 
		// One way is to use table-column names: Space.getColumn("My Table", "My Column").getValue(rowid)

		// Yet, we do not want to resolve names for each access. So we want to store direct references
		// Column col1 = Space.getColumn("My Table", "My Column");
		// Object val1 = col1.getValue(rowid);
		
		// We want to use many columns. There references can be stored in two ways:
		// - In a dictionary or list, which means that we introduce a new referencing system: names or index.
		//   Columns are then accessed, for example, in this way: dict["Amount"].getValue() or list[25].getValue()
		// - In local variables, which means that we have own custom variables and must assign them. 
		//   For example, this class could have variables: col1, col2, amountColumn etc.
		// The best way is that the class decides itself but it has a mechanism for assigning these references. 
		// For example, these column references could be assigned in beginEvaluate or in the constructor. 


		
		
		// We can do row id arithmetics by having access to valid range (but we cannot change these ranges - they will be changed by the driver)
		// It can be needed for rolling functions
		//Range range1 = thisColumn.added; // Dirty, new, to be legally added after they are computed
		//Range range2 = thisColumn.rows; // Clean, have been computed previously
		//Range range3 = thisColumn.removed; // Marked for removal, will be legally removed after this update cycle
		
		// There are two ways how column references can be passed to this class: 
		// - either by the class itself, for example, we always have access to Space and hence can always resolve column names. 
		// - or are provided from outside when the system gets dependency information and we need to store these references wherever we want
		//   - the dependencies can be requested and column references injected from outside. The system anyway must get dependencies (which columns will be used) in order to build execution plan.  
		//   - the dependencies and column resolution can be requested from inside at any time.
		
		
		// We have to return the type that is expected in the declaration of the column:
		// - primitive data type
		// - table data type
		//   - rowid if we have found some record in the output table. currently, we do not have any means for that.
		//   - record object which will be pushed into the output table and the found rowid will be stored as this column value.
		//     - what the system/table will do with the pushed record is defined by the table (push method)
		//     - for export tables, the record will be pushed but null will be returned which we do not need. 
		//       it is a use case for columns which do not want to store their output values because they are not used
		//       these could be specially implemented export columns with mapping config as a function
		//       or the system could determine such columns automatically, for example, if their output is an export (leaf) table
		
		// We could also find a record in the output/type table instead of pushing it.
	}

}
