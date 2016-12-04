package org.conceptoriented.sc.core;

import static org.junit.Assert.*;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.conceptoriented.sc.core.Column;
import org.conceptoriented.sc.core.EvaluatorBase;
import org.conceptoriented.sc.core.Record;
import org.conceptoriented.sc.core.Schema;
import org.conceptoriented.sc.core.Table;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class Tests {

    @BeforeClass
    public static void setUpClass() {
    }

    Schema schema;

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
	// - evaluate schema
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
    public void schemaTest()
    {
    	// Create and configure: schema, tables, columns
        schema = new Schema("My Schema");
        Table table = schema.createTable("T");
        table.maxLength = 2;

        // Data column will get its data from pushed records (input column)
        Column columnA = schema.createColumn("A", "T", "Double");

        // Calculated column. It has a user-defined evaluation method (plug-in, mapping, coel etc.)
        // This column can read its own and other column values, and it knows about new/valid/old record ranges 
        // It is expected to write/update its own value
        // If necessary, it can update its type/output by pushing records to its type/output table and using the returned row id for writing into itself
        Column columnB = schema.createColumn("B", "T", "Double");
        String descr = "{ `class`:`org.conceptoriented.sc.core.EvaluatorB`, `dependencies`:[`A`] }";
        columnB.setDescriptor(descr.replace('`', '"'));

        // Add one or more records to the table
        Record record = new Record();

        record.set("A", 5.0);
        table.append(record); // The number of added/dirty records is incremented. Some records can be marked for deletion/old. 
    	
        record.set("A", 10.0);
        table.append(record); // The number of added/dirty records is incremented. Some records can be marked for deletion/old. 

        // Evaluate schema by updating its schema. Mark new records as clean and finally remove records for deletion.
        schema.evaluate();
        
        record.set("A", 20.0);
        table.append(record); 

        // Evaluate schema by updating its schema. Mark new records as clean and finally remove records for deletion.
        schema.evaluate();
        
        // Check the result

    }
    
    @Test
    public void classLoaderTest() 
    {
    	// Create class loader for the schema
    	// UDF class have to be always in nested folders corresponding to their package: either directly in file system or in jar
    	File classDir = new File("C:/TEMP/classes/");
        URL[] classUrl = new URL[1];
		try {
			classUrl[0] = classDir.toURI().toURL();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		URLClassLoader classLoader = new URLClassLoader(classUrl);
		// Now the schema is expected to dynamically load all class definitions for evaluators by using this class loader from this dir

		
		try {
			Class classB = classLoader.loadClass("org.conceptoriented.sc.core.EvaluatorB");
			Object o = classB.newInstance();
			classB = null;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    	schema = new Schema("My Schema");
		schema.setClassLoader(classLoader);

        Table table = schema.createTable("T");
        table.maxLength = 2;

        // Data column will get its data from pushed records (input column)
        Column columnA = schema.createColumn("A", "T", "Double");

        Column columnB = schema.createColumn("B", "T", "Double");
        columnB.setDescriptor("{ \"class\":\"org.conceptoriented.sc.core.EvaluatorB\" }");
        
        
    }

    @Test
    public void tupleParserTest() 
    {
    	QNameBuilder nb = new QNameBuilder();
    	QName name = nb.buildQName("[a1 1 1].b222");

    	ExprNode t = new ExprNode(); 

    	t.formula = "{ bbb = v11 + 1 * sin(bla) / bla ; [ccc]=22,2 }";
    	t.name = "aaa";
    	t.parse();

    	t.formula = "{ bbb = v11 + 1 * sin(bla) ; [ccc]= {ddd=22,2} }";
    	t.name = "aaa";
    	t.parse();
    	t = null;
    }

    @Test
    public void primExprTest() 
    {
    	schema = new Schema("My Schema");
        Table table = schema.createTable("T");
        Column columnA = schema.createColumn("A", "T", "Double");
        Column columnB = schema.createColumn("B", "T", "Double");
        
        Record record = new Record();
        record.set("A", 5.0);
        table.append(record);
        record.set("A", null);
        table.append(record);
        record.set("A", 6);
        table.append(record);
        
        columnB.setFormula("2 * [A] + 1");

        columnB.translate();
        
        // Check correctness of dependencies
        List<Column> depsB = schema.getDependency(columnB);
        assertTrue( depsB.contains(columnA) );

        columnB.evaluate();

        assertEquals(11.0, (Double)columnB.getValue(0), 0.00001);
        assertEquals(Double.NaN, columnB.getValue(1));
        assertEquals(13.0, (Double)columnB.getValue(2), 0.00001);
    }

    @Test
    public void tupleExprTest()
    {
    	// Create and configure: schema, tables, columns
        schema = new Schema("My Schema");

        //
        // Table 1 (type table)
        //
        Table t1 = schema.createTable("T");

        Column c1 = schema.createColumn("A", "T", "Double");
        Column c2 = schema.createColumn("B", "T", "Double");

        // Add one or more records to the table
        Record r = new Record();

        r.set("A", 5.0);
        r.set("B", 10.0);
        t1.append(r); 

        //
        // Table 2
        //
        Table t2 = schema.createTable("T2");

        Column c3 = schema.createColumn("A", "T2", "Double");
        Column c4 = schema.createColumn("B", "T2", "Double");

        Column c5 = schema.createColumn("C", "T2", "T");

        // Add one or more records to the table
        r = new Record();

        r.set("A", 5.0);
        r.set("B", 10.0);
        t2.append(r); 
    	
        r.set("A", 10.0);
        r.set("B", 5.0);
        t2.append(r); 

        c5.setFormula(" { [A] = [A]; [B] = [B] } ");

        c5.translate();

        // Check correctness of dependencies
        List<Column> depsC5 = schema.getDependency(c5);
        assertTrue( depsC5.contains(c3) );
        assertTrue( depsC5.contains(c4) );

        c5.evaluate();

        assertEquals(0L, c5.getValue(0));
        assertEquals(1L, c5.getValue(1));
    }
    
    @Test
    public void accuExprTest()
    {
    	// Create and configure: schema, tables, columns
        schema = new Schema("My Schema");

        //
        // Table 1 (group table)
        //
        Table t1 = schema.createTable("T");

        Column tid = schema.createColumn("Id", "T", "Double");

        // Add one or more records to the table
        Record r = new Record();

        r.set("Id", 5.0);
        t1.append(r); 
        r.set("Id", 10.0);
        t1.append(r); 
        r.set("Id", 15.0);
        t1.append(r); 

        // Define accu column
        Column ta = schema.createColumn("A", "T", "Double");
        ta.setFormula(""); // Init to default

        ta.setAccutable("T2");
        ta.setAccuformula(" output + 2.0 * [Id] ");
        ta.setAccupath("[G]");

        //
        // Table 2 (fact table)
        //
        Table t2 = schema.createTable("T2");

        Column t2id = schema.createColumn("Id", "T2", "Double");

        // Add one or more records to the table
        r = new Record();

        r.set("Id", 5.0);
        t2.append(r); 
        r.set("Id", 5.0);
        t2.append(r); 
        r.set("Id", 10.0);
        t2.append(r); 
        r.set("Id", 20.0);
        t2.append(r);

        // Define group column
        Column t2g = schema.createColumn("G", "T2", "T");
        t2g.setFormula(" { [Id] = [Id] } ");

        //
        // Translate and evaluate
        //
        schema.translate();

        // Check correctness of dependencies
        List<Column> depsTa = schema.getDependency(ta);
        assertTrue( depsTa.contains(t2id) ); // Used in formula
        assertTrue( depsTa.contains(t2g) ); // Group path

        schema.evaluate();

        assertEquals(20.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));
    }
}
