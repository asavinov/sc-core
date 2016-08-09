package org.conceptoriented.sc.core;

public enum DcErrorCode {
	GENERAL(0), NOT_FOUND_IDENTITY(1), GET_ELEMENT(2), CREATE_ELEMENT(3), UPATE_ELEMENT(4), DELETE_ELEMENT(5)
	;

	private int value;

	public int getValue() {
		return value;
	}

	private DcErrorCode(int value) {
		this.value = value;
	}
}
