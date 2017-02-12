package org.conceptoriented.sc.core;

public enum DcColumnKind {
	NONE(10), // Not specified (missing)
	UNKNOWN(20), // Cannot determine

	AUTO(0), // Auto. Best kind has to be determined from other parameters. 

	USER(50), // Values are provided by the user or in any case from outside, that is, not derived. 
	CALC(60), // Calculated (row-based, no accumulation, non-complex)
	ACCU(70), // Accumulation
	LINK(80), // Tuple (complex)
	CLASS(100), // Java class
	;

	private int value;

	public int getValue() {
		return value;
	}

	public static DcColumnKind fromInt(int value) {
	    for (DcColumnKind kind : DcColumnKind.values()) {
	        if (kind.getValue() == value) {
	            return kind;
	        }
	    }
	    return DcColumnKind.AUTO;
	 }

	private DcColumnKind(int value) {
		this.value = value;
	}
}
