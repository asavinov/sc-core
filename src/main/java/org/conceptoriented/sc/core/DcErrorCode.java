package org.conceptoriented.sc.core;

public enum DcErrorCode {
	NONE(0), 
	GENERAL(1), 
	NOT_FOUND_IDENTITY(10), 
	GET_ELEMENT(21), CREATE_ELEMENT(22), UPATE_ELEMENT(23), DELETE_ELEMENT(24),
	PARSE_ERROR(51), BIND_ERROR(52), EVALUATE_ERROR(53),
	PARSE_PROPAGATION_ERROR(61), BIND_PROPAGATION_ERROR(62), EVALUATE_PROPAGATION_ERROR(63),
	;

	private int value;

	public int getValue() {
		return value;
	}

	private DcErrorCode(int value) {
		this.value = value;
	}
}
