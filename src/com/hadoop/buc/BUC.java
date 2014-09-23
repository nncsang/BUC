package com.hadoop.buc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class BUC {
	private String[] input;
	private int numDims;
	private int minsup;
	private List<Integer> dataCount;
	private InputReader reader;
	private String[] attributeNames;
	private String measuredAttributeName;
	private List<List<Integer>> outputRec;
	private Map<String, Integer> cubeGroups;
	private Set<String> cubeRegions;
	
	public BUC(String[] input, String[] attributeNames, String measuredAttributeName, int numDims, int minsup, InputReader reader){
		this.input = input;
		this.numDims= numDims;
		this.minsup = minsup;
		this.reader = reader;
		this.attributeNames = attributeNames;
		this.measuredAttributeName = measuredAttributeName;
		
		dataCount = new ArrayList<Integer>();
		for(int i = 0; i < numDims; i++)
			dataCount.add(0);
	}
	
	public Map<String, Integer> cubeGroups(){
		if (cubeGroups != null && cubeGroups.isEmpty())
			cubeGroups.clear();
		else
			cubeGroups = new TreeMap<String, Integer>();
			
		processCubeGroups(input, 0, 0, 0);
		return cubeGroups;
	}
	
	public Set<String> cubeRegions(){
		if (cubeRegions != null && cubeRegions.isEmpty())
			cubeRegions.clear();
		else
			cubeRegions = new TreeSet<String>();
		
		processCubeRegions(0, 0, 0);
		return cubeRegions;
	}
	
	public int aggregate(String[] input){
		int sum = 0;
		for(int i = 0; i < input.length; i++){
			reader.initWithString(input[i]);
			sum += Integer.parseInt(reader.getValueByAttributeName(measuredAttributeName));
		}
		return sum;
	}
	
	public static String join(String[] list, String delim) {
	    int len = list.length;
	    if (len == 0)
	        return "";
	    StringBuilder sb = new StringBuilder(list[0].toString());
	    for (int i = 1; i < len; i++) {
	        sb.append(delim);
	        sb.append(list[i].toString());
	    }
	    return sb.toString();
	}
	
	public static String join(List<String> list, String delim) {
	    int len = list.size();
	    if (len == 0)
	        return "";
	    StringBuilder sb = new StringBuilder(list.get(0).toString());
	    for (int i = 1; i < len; i++) {
	        sb.append(delim);
	        sb.append(list.get(i).toString());
	    }
	    return sb.toString();
	}
	
	public void processCubeGroups(String[] input, int mainOrigin, int origin, int dim){
		int result = aggregate(input);
		
		if (dim == 0){
			String[] region = new String[numDims];
			for(int i = 0; i < numDims; i++){
				region[i] = "*";
			}
			
			String key = join(region, ",");
			if (!cubeGroups.containsKey(key))
				cubeGroups.put(key, result);
		}
		else{
			if (dim <= numDims){
				reader.initWithString(input[0]);
				
				String[] region = new String[numDims];
				for(int i = 0; i < origin; i++){
					region[i] = "*";
				}
				
				for(int i = origin; i < dim; i++){
					region[i] = reader.getValueByAttributeName(attributeNames[i]);
				}
				
				for(int i = dim; i < numDims; i++){
					region[i] = "*";
				}
			
				String key = join(region, ",");
				if (!cubeGroups.containsKey(key))
					cubeGroups.put(key, result);
			}
		}
		
		for(int i = dim; i < numDims; i++){
			Map<String, Integer> partitions = partition(input, i);
			Set<Entry<String, Integer>> entries = partitions.entrySet();
			for(Entry<String, Integer> entry : entries){
				String key = entry.getKey();
				int value = entry.getValue();
				
				if (value >= minsup){
					List<String> newInput = new ArrayList<String>();
					for(int j = 0; j < input.length; j++){
						reader.initWithString(input[j]);
						String keyOfRecord = reader.getValueByAttributeName(attributeNames[i]);
						if (key.equals(keyOfRecord)){
							newInput.add(input[j]);
						}	
					}
					
					processCubeGroups(newInput.toArray(new String[0]), -1, origin, i + 1);
				}
			}
			if (mainOrigin == 0)
				origin = i + 1;
		}
	}
	
	public void processCubeRegions(int mainOrigin, int origin, int dim){
		
		if (dim == 0){
			String[] region = new String[numDims];
			for(int i = 0; i < numDims; i++){
				region[i] = "*";
			}
			cubeRegions.add(join(region, ","));
		}
		else{
			if (dim <= numDims){
				String[] region = new String[numDims];
				for(int i = 0; i < origin; i++){
					region[i] = "*";
				}
				
				for(int i = origin; i < dim; i++){
					region[i] = attributeNames[i];
				}
				
				for(int i = dim; i < numDims; i++){
					region[i] = "*";
				}
			
				cubeRegions.add(join(region, ","));
			}
		}
		
		for(int i = dim; i < numDims; i++){
			processCubeRegions(-1, origin, i + 1);
			if (mainOrigin == 0)
				origin = i + 1;
		}
	}
	
	public void printCubeGroups(){
		if (cubeGroups == null || cubeGroups.isEmpty())
			return;
		Set<Entry<String, Integer>> entries = cubeGroups.entrySet();
		
		for(Entry<String, Integer> entry : entries){
			System.out.println("(" + entry.getKey() + ") " + entry.getValue());
		}
	}
	
	public void printCubeRegions(){
		if (cubeRegions == null || cubeRegions.isEmpty())
			return;
		Iterator<String> itor = cubeRegions.iterator();
		while(itor.hasNext()){
			System.out.println(itor.next());
		}
	}
	
	public Map<String, Integer> partition(String[] input, int dims){
		Map<String, Integer> partitions = new HashMap<String, Integer>();
		for(String record : input){
			reader.initWithString(record);
			String key = reader.getValueByAttributeName(attributeNames[dims]);
			String value = reader.getValueByAttributeName(measuredAttributeName);
			if (partitions.containsKey(key)){
				partitions.put(key, partitions.get(key) + Integer.parseInt(value));
			}else{
				partitions.put(key, Integer.parseInt(value));
			}
		}
		return partitions;	
	}
	
	static public void main(String[] args){
		List<String> input = new ArrayList<String>();
		try{
			BufferedReader br = new BufferedReader(new FileReader("input.txt"));
		    String line;
		    while ((line = br.readLine()) != null) {
		       input.add(line);
		    }
		    br.close();
		}catch(Exception ex){
			System.out.println(ex.getMessage());
			return;
		}
		
		String[] attributeNames = {"Item", "Color", "Store"}; 
		BUC buc = new BUC(input.toArray(new String[0]), attributeNames, "Quantity", 3, 0, new InventoryReader());
		buc.cubeGroups();
		buc.printCubeGroups();
//		buc = new BUC(null, attributeNames, "Quantity", 3, 0, null);
//		buc.cubeRegions();
//		buc.printCubeRegions();
	}
}
