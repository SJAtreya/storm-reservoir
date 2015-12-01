package com.reservoir.test.storm;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.reservoir.test.storm.util.Util;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class ReservoirSpout extends BaseRichSpout {

	private SpoutOutputCollector _collector;
	private AtomicLong currentVolume;
	private AtomicLong maxCapacity;
	private Long input = 10000000l;
	private Long subReservoirCount = 0l;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		currentVolume = new AtomicLong(0l);
		subReservoirCount = (Long) conf.get("subCount");
		maxCapacity = new AtomicLong((Long) conf.get("maxCapacity"));
		System.out.println("Setting spout capacity!");
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		Utils.sleep(1000);
		currentVolume.addAndGet(input);
		Double diff = Util
				.currentVolume(currentVolume.get(), maxCapacity.get());
		System.out.println("Main Reservoir current volume:" + currentVolume
				+ ", Max Volume:" + maxCapacity + ", storage at:" + diff
				+ "%, time to full:"
				+ ((maxCapacity.get() - currentVolume.get()) / input));
		if (diff >= 80d) {
			System.out.println("Main Reservoir greater than threshold:" + diff);
			_collector.emit(new Values(input / subReservoirCount));
			currentVolume.addAndGet(-input);
			System.out
					.println("Released "
							+ input
							+ " cubic m of water from main reservoir to sub reservoirs:"
							+ diff);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("flow"));
	}

}
