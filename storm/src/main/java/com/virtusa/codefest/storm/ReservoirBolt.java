package com.virtusa.codefest.storm;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.virtusa.codefest.storm.util.Util;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ReservoirBolt implements IRichBolt {
	private OutputCollector _collector;
	private AtomicLong currentVolume;
	private AtomicLong maxCapacity;
	private String name;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		name = (String) stormConf.get("Name");
		maxCapacity = new AtomicLong((Long) stormConf.get("maxCapacity"));
		currentVolume = new AtomicLong(0l);
		System.out.println("Setting " + name + " capacity!");
		_collector = collector;
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		System.out.println(name + " reservoir received :" + input.getLong(0)
				+ "cubic m of water!");
		currentVolume.addAndGet(input.getLong(0));
		System.out
				.println(name
						+ " reservoir current volume:"
						+ currentVolume
						+ ", Max Volume:"
						+ maxCapacity
						+ ", time to full:"
						+ ((maxCapacity.get() - currentVolume.get()) / input
								.getLong(0)));
		Double diff = 0d;
		if ((diff = Util.currentVolume(currentVolume.get(), maxCapacity.get())) >= 80d) {
			System.out.println(name + " Reservoir greater than threshold:"
					+ diff);
			_collector.emit(new Values(input.getLong(0)));
			currentVolume.addAndGet(-input.getLong(0));
			System.out.println("Released " + input + " cubic m of water from "
					+ name + " reservoir to downstream reservoirs:" + diff);
		}

	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		// if (child!=null && child.isEmpty()) {
		// declarer.declare(new Fields(child));
		// }
		declarer.declare(new Fields("flow"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
