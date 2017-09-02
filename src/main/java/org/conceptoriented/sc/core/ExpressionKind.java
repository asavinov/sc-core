package org.conceptoriented.sc.core;

public enum ExpressionKind {
	NONE(0), // No formula. For example, use directly evaluator object 

	AUTO(10), // Auto. Formula kind has to be determined automatically using other parameters. 

	EXP4J(20), // Like "[Column 1] + [Column 2] / 2.0"
	EVALEX(30),

	JAVASCRIPT(40), 

	UDE(50), // For example, "{ class: "com.package.MyUde.class", parameters: [ "Column1", "[Column 2].[Column 3]" ] }"
	;

	private int value;

	public int getValue() {
		return value;
	}

	public static ExpressionKind fromInt(int value) {
	    for (ExpressionKind kind : ExpressionKind.values()) {
	        if (kind.getValue() == value) {
	            return kind;
	        }
	    }
	    return ExpressionKind.AUTO;
	 }

	private ExpressionKind(int value) {
		this.value = value;
	}
}