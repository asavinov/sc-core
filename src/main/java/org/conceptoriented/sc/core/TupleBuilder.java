package org.conceptoriented.sc.core;

import java.util.List;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

public class TupleBuilder extends TupleBaseVisitor<FunctionExpr> {

	@Override 
	public FunctionExpr visitTuple(TupleParser.TupleContext ctx) 
	{
		// It is called if we meet {}
		// We need to loop over all members and return a list of tuples without name
		
        FunctionExpr n = new FunctionExpr();

        // Find all members and store them in the tuple node
        int mmbrCount = ctx.member().size();
        for (int i = 0; i < mmbrCount; i++)
        {
            FunctionExpr mmbr = visit(ctx.member(i));
            if (mmbr != null)
            {
                n.tuple.add(mmbr);
            }
        }

        return n; 
	}

	@Override 
	public FunctionExpr visitMember(TupleParser.MemberContext ctx) 
	{ 
		// It is called if we meet assignment within a tuple
		// We need to create a new tuple object from this assignment
		// The value can be either another tuple or string (terminal expression)

        // Determine declared member (constituent, offset, parameter) name
        String name = ctx.getText();
        //String name = GetName(ctx.name());
        
        FunctionExpr n;
        if(ctx.getChild(2) instanceof TupleParser.TupleContext) { // Value is a tuple
        	// Get the tuple
            n = new FunctionExpr();
        	// Assign name to id
            n.name = name;
        }
        else { // Value is a (terminal) expression
            n = new FunctionExpr();
            n.name = name;
            //n.expression = ctx.expr().getText();
        }

		return n; 
	}

    protected String GetName(/*TupleParser.NameContext context*/)
    {
        String name=null;
        /*
        if (context.DELIMITED_ID() != null)
        {
            name = context.DELIMITED_ID().getText();
            name = name.substring(1, name.length() - 1); // Remove delimiters
        }
        else
        {
            name = context.ID().getText();
        }
        */

        return name;
    }

	/**
	 * Convenience method for parsing strings.
	 */
	public FunctionExpr buildTuple(String str)
    {
        TupleBuilder builder = this;

        TupleLexer lexer;
        TupleParser parser;
        ParseTree tree;
        String tree_str;
        FunctionExpr ast;

        lexer = new TupleLexer(new ANTLRInputStream(str));
        parser = new TupleParser(new CommonTokenStream(lexer));
        tree = parser.tuple();
        tree_str = tree.toStringTree(parser);

        ast = builder.visit(tree);

        return ast;
    }

}
