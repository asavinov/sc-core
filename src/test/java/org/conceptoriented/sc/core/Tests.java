package org.conceptoriented.sc.core;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.conceptoriented.sc.core.Column;
import org.conceptoriented.sc.core.EvaluatorBase;
import org.conceptoriented.sc.core.Record;
import org.conceptoriented.sc.core.Space;
import org.conceptoriented.sc.core.Table;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class Tests {

    @BeforeClass
    public static void setUpClass() {
    }

    Space space;

    @Before
    public void setUp() {
    }


	// Major steps
	// - create schema objects either programmatically or by de-serializing from JSON:
	// - configure schema objects. Importantly, every column has to get its evaluator instance: instance of plug-in, instance of COEL expression, instance of Mapping specification etc.
	//   - normal columns either explicitly assigned a plug-in or find this plug-in by name convention. 
	//   - import columns need to be configured my using Mapping configuration so that their evaluate function can pop records from input stream and push then into the type table.
	//     it also can use a (simple) filter so that not all input records are imported. A mapping is simply an output record with fields defined in terms of the input column values (COEL TUPLE) but it can be more complex in the case of complex import object structure (JSON, XML, audio etc.).
	//     Import mapping specification can be executed in terms of the specific Record implementation which it exposes (rather than column structure). 
	//   - export columns are normal user-defined columns which return a record and push it to the type table (which can be export table)
	//   - export columns can be a special (export) column which accepts mapping as its formula. for example, it will read certain columns and then push a record to the output.
	//     the only problem is that we do not want to output all records so we need a mechanism of filtering for mapping
	//     filter can be specified as some other binary column (so it depends on it) computed by the user: if it is true then this export column outputs a record according to the mapping
	//
	// - evaluate space
	//   - retrieve dependencies from all column functions
	//   - build dependency graph
	//   - initialize column functions by passing column references and other parameters needed for evaluation.
	//   - execute all column evaluations.
	//     - a column might not need to be evaluated if it is not dirty (it is determined either by the driver or the column evaluator itself)
	//     - import columns determine their dirty status from the import table (if its has new record)
	//     - evaluation of import columns will add new records to normal tables by making them dirty
	//     - normal columns can also append records to other tables by making them dirty
	//     - records can be also appended to export tables but here nothing happens (the export thread will consume them)
	//       we never write to export explicitly - we need to return the corresponding object to be pushed. 


    @Test
    public void SchemaTest()
    {
    	// Create and configure: space, tables, columns
        space = new Space("My Space");
        Table table = space.createTable("T");
        table.maxRows = 2;

        // Data column will get its data from pushed records (input column)
        Column columnA = space.createColumn("A", "T", "Double");

        // Calculated column. It has a user-defined evaluation method (plug-in, mapping, coel etc.)
        // This column can read its own and other column values, and it knows about new/valid/old record ranges 
        // It is expected to write/update its own value
        // If necessary, it can update its type/output by pushing records to its type/output table and using the returned row id for writing into itself
        Column columnB = space.createColumn("B", "T", "Double");
        EvaluatorBase evaluator = new EvaluatorB();
        evaluator.thisColumn = columnB;
        Map<Object,Column> deps = new HashMap<Object,Column>();
        deps.put("A", columnA);
        evaluator.setColumns(deps);

        columnB.setEvaluator(evaluator);
    	
        // Add one or more records to the table
        Record record = new Record();

        record.set("A", 5.0);
        table.push(record); // The number of added/dirty records is incremented. Some records can be marked for deletion/old. 
    	
        record.set("A", 10.0);
        table.push(record); // The number of added/dirty records is incremented. Some records can be marked for deletion/old. 

        // Evaluate space by updating its space. Mark new records as clean and finally remove records for deletion.
        space.evaluate();
        
        record.set("A", 20.0);
        table.push(record); 

        // Evaluate space by updating its space. Mark new records as clean and finally remove records for deletion.
        space.evaluate();
        
        // Check the result

    }

}
