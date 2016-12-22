package org.conceptoriented.sc.core;

import static org.junit.Assert.*;

import java.io.File;
import java.math.BigDecimal;
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

    @Test
    public void evalExTest()
    {
    	BigDecimal result = null;
    	
    	com.udojava.evalex.Expression e = new com.udojava.evalex.Expression("1+1/3");
    	e.setPrecision(2);
    	e.setRoundingMode(java.math.RoundingMode.UP);
    	result = e.eval();
    	
    	e = new com.udojava.evalex.Expression("SQRT(a^2 + b^2)");
    	List<String> usedVars = e.getUsedVariables();

    	e.getExpressionTokenizer(); // Does not detect errors
    	
    	e.setVariable("a", "2.4"); // Can work with strings (representing numbers)
    	e.setVariable("b", new BigDecimal(9.253));

    	// Validate
    	try {
        	e.toRPN(); // Generates prefixed representation but can be used to check errors (variables have to be set in order to correctly determine parse errors)
    	}
    	catch(com.udojava.evalex.Expression.ExpressionException ee) {
    		System.out.println(ee);
    	}

    	result = e.eval();
		
		result = new com.udojava.evalex.Expression("random() > 0.5").eval();

		//e = new com.udojava.evalex.Expression("MAX('aaa', 'bbb')");
		// We can define custom functions but they can take only numbers (as constants). 
		// EvalEx does not have string parameters (literals). 
		// It does not recognize quotes. So maybe simply introduce string literals even if they will be converted into numbers, that is, just like string in setVariable.
		// We need to change tokenizer by adding string literals in addition to numbers and then their processing.
		
		e.eval();
    }

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
        List<Column> depsB = schema.getParentDependencies(columnB);
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
        List<Column> depsC5 = schema.getParentDependencies(c5);
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
        List<Column> depsTa = schema.getParentDependencies(ta);
        assertTrue( depsTa.contains(t2id) ); // Used in formula
        assertTrue( depsTa.contains(t2g) ); // Group path

        schema.evaluate();

        assertEquals(20.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));
    }
}
