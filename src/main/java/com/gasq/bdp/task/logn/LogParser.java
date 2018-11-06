package com.gasq.bdp.task.logn;

import com.gasq.bdp.task.GasqSparkTask;

public interface LogParser extends GasqSparkTask {

	public <T> T parseLine(String line, String TERMINATED);

}
