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
		
		List<Integer> mask = new ArrayList<Integer>();
		for(int i = 0; i < numDims; i++)
			mask.add(0);
		
		processCubeGroups(input, 0, 0, 0, mask);
		return cubeGroups;
	}

	public Set<String> cubeRegions(){
		if (cubeRegions != null && cubeRegions.isEmpty())
			cubeRegions.clear();
		else
			cubeRegions = new TreeSet<String>();
		
		List<Integer> mask = new ArrayList<Integer>();
		for(int i = 0; i < numDims; i++)
			mask.add(0);
		
		processCubeRegions(0, 0, 0, mask);
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
	
	public void processCubeGroups(String[] input, int mainOrigin, int origin, int dim, List<Integer> mask){
		int result = aggregate(input);
		addGroup(mask, input[0], result);
		
		if (dim < numDims){
			mask.set(dim, 1);
		}
		
		for(int i = dim; i < numDims; i++){
			if (mainOrigin == 0){
				for(int k = 0; k < numDims; k++)
					mask.set(k, 0);
				mask.set(origin, 1);
			}
			
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
					
					List<Integer> maskX = new ArrayList(mask);
					processCubeGroups(newInput.toArray(new String[0]), -1, origin, i + 1, maskX);
				}
			}
			if (mainOrigin == 0)
				origin = i + 1;
		}
	}
	
	public void addRegion(List<Integer> mask){
		Iterator<Integer> itor = mask.iterator();
		List<String> region = new ArrayList<String>();
		
		int index = 0;
		while(itor.hasNext()){
			if (itor.next() == 0)
				region.add("*");
			else
				region.add(attributeNames[index]);
			index++;
		}
		
		cubeRegions.add(join(region, ","));
		//System.out.println(join(region, ","));
	}
	
	public void addGroup(List<Integer> mask, String record, int measure){
		Iterator<Integer> itor = mask.iterator();
		List<String> region = new ArrayList<String>();
		reader.initWithString(record);
		int index = 0;
		while(itor.hasNext()){
			if (itor.next() == 0)
				region.add("*");
			else
				region.add(reader.getValueByAttributeName(attributeNames[index]));
			index++;
		}
		
		String key = join(region, ",");
		if (!cubeGroups.containsKey(key))
			cubeGroups.put(key, measure);
		//System.out.println(join(region, ",") + " " + measure);
	}
	
	public void processCubeRegions(int mainOrigin, int origin, int dim, List<Integer> mask){
		addRegion(mask);
		
		for(int i = dim; i < numDims; i++){
			if (mainOrigin == 0 && i == 0){
				mask.set(0, 1);
				addRegion(mask);
			}
			
			for(int j = i + 1; j < numDims; j++){
				if (i != j){
					mask.set(j, 1);
					processCubeRegions(-1, origin, j, mask);
					mask.set(j, 0);
				}
			}
			
			if (mainOrigin == 0){
				origin = i + 1;
				if (origin < numDims){
					for(int k = 0; k < numDims; k++)
						mask.set(k, 0);
					mask.set(origin, 1);
					addRegion(mask);
				}
			}
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
