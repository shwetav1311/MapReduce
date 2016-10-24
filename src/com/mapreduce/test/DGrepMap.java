package com.mapreduce.test;

public class DGrepMap implements Mapper{

	
	public DGrepMap() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public String map(String line, String text) {
		// TODO Auto-generated method stub
		
		if(line.contains(text))
			return line;
		else
			return null;
	}
}
