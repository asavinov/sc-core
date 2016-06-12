package org.conceptoriented.sc;

import static org.junit.Assert.*;

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

    @Test
    public void SchemaTest()
    {
    	// Create and configure: space, tables, columns
        space = new Space();
        Table table = space.createTable("T");

        // Data column will get its data from pushed records (input column)
        Column columnA = space.createColumn("A", "T", "Double");

        // Calculated column. It has a user-defined evaluation method (plug-in, mapping, coel etc.)
        // This column can read its own and other column values, and it knows about new/valid/old record ranges 
        // It is expected to write/update its own value
        // If necessary, it can update its type/output by pushing records to its type/output table and using the returned row id for writing into itself
        Column columnB = space.createColumn("B", "T", "Double");
        columnB.setEvaluator(new EvaluatorB());
    	
        // Add one or more records to the table
        Record record = new Record();
        record.set("A", 5.0);
        table.push(record); // The number of added/dirty records is incremented. Some records can be marked for deletion/old. 
    	
    	// Evaluate space by updating its space. Mark new records as clean and finally remove records for deletion.
        space.evaluate();
    }

}
