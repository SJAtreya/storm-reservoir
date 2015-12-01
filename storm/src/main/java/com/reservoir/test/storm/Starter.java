package com.reservoir.test.storm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.reservoir.test.data.MySQLCM;
import com.reservoir.test.storm.util.Util;

public class Starter {
	private final static String MAIN_RESERVOIR = "Main Reservoir";

	private final static Long RESERVOIR_LENGTH_IN_M = 500l;
	private final static Long RESERVOIR_BREADTH_IN_M = 2000l;
	private final static Long RESERVOIR_DEPTH_IN_M = 100l;

	private final static Long SUB_RESERVOIR_LENGTH_IN_M = 250l;
	private final static Long SUB_RESERVOIR_BREADTH_IN_M = 1000l;
	private final static Long SUB_RESERVOIR_DEPTH_IN_M = 50l;

	public static void main(String[] args) throws Exception {
		Map<String, List<String>> configMap = getConfiguration();
		Long reservoirMaxCapacity = Util.getMaxCapacity(RESERVOIR_LENGTH_IN_M,
				RESERVOIR_BREADTH_IN_M, RESERVOIR_DEPTH_IN_M);
		Long subReservoirMaxCapacity = Util.getMaxCapacity(
				SUB_RESERVOIR_LENGTH_IN_M, SUB_RESERVOIR_BREADTH_IN_M,
				SUB_RESERVOIR_DEPTH_IN_M);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new ReservoirSpout())
				.addConfiguration("maxCapacity", reservoirMaxCapacity)
				.addConfiguration("subCount",
						configMap.get(MAIN_RESERVOIR).size());
		for (String bolt : configMap.get(MAIN_RESERVOIR)) {
			builder.setBolt(bolt, new ReservoirBolt()).shuffleGrouping("spout")
					.addConfiguration("Name", bolt)
					.addConfiguration("maxCapacity", subReservoirMaxCapacity);
		}
		configMap.remove(MAIN_RESERVOIR);
		for (Map.Entry<String, List<String>> configs : configMap.entrySet()) {
			BoltDeclarer declarer = builder.setBolt(configs.getKey(),
					new ReservoirBolt());
			declarer.addConfiguration("Name", configs.getKey());
			declarer.addConfiguration("maxCapacity", subReservoirMaxCapacity);
			for (String parent : configs.getValue()) {
				declarer.shuffleGrouping(parent);
			}
		}
		Config conf = new Config();
		conf.setDebug(false);
		conf.put(Config.TOPOLOGY_DEBUG, false);
		conf.setNumWorkers(4);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
	}

	private static Map<String, List<String>> getConfiguration()
			throws SQLException {
		Connection conn = MySQLCM.getConnection();
		ResultSet rs = conn
				.prepareStatement(
						"select (select hubs from master where master.id = source) as source, (select hubs from master where master.id = destination) as destination from links")
				.executeQuery();
		Map<String, List<String>> map = new HashMap<String, List<String>>();
		String key, value;
		while (rs.next()) {
			key = rs.getString(1);
			if (MAIN_RESERVOIR.equals(key)) {
				value = rs.getString(2);
			} else {
				key = rs.getString(2);
				value = rs.getString(1);
			}

			if (map.containsKey(key)) {
				map.get(key).add(value);
			} else {
				List<String> dataList = new ArrayList<String>();
				dataList.add(value);
				map.put(key, dataList);
			}
		}
		MySQLCM.returnConnection(conn);
		System.out.println("MainConfiguration:" + map);
		return map;
	}
}
