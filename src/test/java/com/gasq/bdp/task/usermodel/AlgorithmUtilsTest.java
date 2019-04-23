package com.gasq.bdp.task.usermodel;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.gasq.bdp.task.util.AlgorithmUtils;
import com.gasq.bdp.task.util.DelimiterType;
import com.gasq.bdp.task.util.FileUtil;

public class AlgorithmUtilsTest {
	
	private static List<String> caculateRecommend(List<String> old) throws Exception {
		Stream<Integer> scores = old.stream()
				.map(line -> {
						String[] vals = line.split(DelimiterType.defaultHive.getValue());
						return (vals.length == 5 || vals[4] == null) ? Integer.parseInt(vals[4]) : 0;
				});
		List<Integer> scoreList = scores.collect(Collectors.toList()); 
		Optional<Integer> sum = scoreList.stream().reduce(Integer::sum);
		System.out.println("******************:" + scoreList.size());
		Map<String, Double> recommend =  AlgorithmUtils.calculationProjectsRecommend(scoreList.size(), new BigDecimal(sum.get()), scoreList);
		double max = Double.parseDouble(recommend.get("maxinterval").toString());
		double min = Double.parseDouble(recommend.get("mininterval").toString());
		return old.stream().filter(x -> {
					if(x == null)
						return false;
					String[] vals = x.split(DelimiterType.defaultHive.getValue());
					return (vals.length == 5 && vals[4] != null && 
							Double.parseDouble(vals[4]) > min && Double.parseDouble(vals[4]) < max);
					}).collect(Collectors.toList());
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		List<String> lines = FileUtil.readLine("E:\\tmp\\hello\\usermodel\\9141cd8cb83766b5-32354eb00000000a_1707226898_data.0");
//		Integer[] scores = {4,3,1,1,1,1,1,1,1,1,1,1,3,5,1,1};
//		List<Integer> lines = Arrays.asList(scores);
		int i = 0;
		while(i < 3) {
			lines = caculateRecommend(lines);
			i++;
		}
		FileUtil.write(lines, "E:\\tmp\\hello\\usermodel\\result.hive");
	}

}
