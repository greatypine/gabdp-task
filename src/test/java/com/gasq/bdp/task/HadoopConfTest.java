package com.gasq.bdp.task;

import org.apache.hadoop.conf.Configuration;

public class HadoopConfTest implements GasqTask {

	public static void main(String[] args) {
		HadoopConfTest test = new HadoopConfTest();
		Configuration conf = test.getConf();
		System.out.println(conf.get("fs.defaultFS"));

	}

}
