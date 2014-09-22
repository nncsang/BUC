package com.hadoop.buc;

public class InventoryReader implements InputReader{
	String[] sources;
	
	@Override
	public void initWithString(String input) {
		sources = input.split("\t");
		
	}

	@Override
	public String getValueByAttributeName(String name) {
		
		if (name.equals("Item"))
			return sources[0];
		if (name.equals("Color"))
			return sources[1];
		if (name.equals("Quantity"))
			return sources[2];
		
		return null;
	}

}
