package com.hadoop.buc;

public interface InputReader {
	public void initWithString(String input);
	public String getValueByAttributeName(String name);
}
