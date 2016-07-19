package org.conceptoriented.sc.core;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

public class QNameBuilder extends QNameBaseVisitor<QName> {

	@Override 
	public QName visitQname(QNameParser.QnameContext ctx) { 
		QName n = new QName();
		
        int nameCount = ctx.name().size();
        for (int i = 0; i < nameCount; i++)
        {
        	String name = GetName(ctx.name(i));
        	n.names.add(name);
        }
        
        return n;
	}

    protected String GetName(QNameParser.NameContext context)
    {
        String name;
        if (context.DELIMITED_ID() != null)
        {
            name = context.DELIMITED_ID().getText();
            name = name.substring(1, name.length() - 1); // Remove delimiters
        }
        else
        {
            name = context.ID().getText();
        }

        return name;
    }

	public QName buildQName(String str)
    {
        QNameBuilder builder = this;

        QNameLexer lexer;
        QNameParser parser;
        ParseTree tree;
        String tree_str;
        QName ast;

        lexer = new QNameLexer(new ANTLRInputStream(str));
        parser = new QNameParser(new CommonTokenStream(lexer));
        tree = parser.qname();
        tree_str = tree.toStringTree(parser);

        ast = builder.visit(tree);

        return ast;
    }

}
