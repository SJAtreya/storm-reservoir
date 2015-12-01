package com.reservoir.test.storm.util;

public class Util {

	public static Long getMaxCapacity(long length, long breadth, long depth) {
		return length * breadth * depth;
	}

	public static Double currentVolume(Long currentCapacity, Long maxCapacity) {
		return (currentCapacity.doubleValue() / maxCapacity.doubleValue()) * 100;
	}
}
