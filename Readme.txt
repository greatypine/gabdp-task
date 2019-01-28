gabdp-task 项目重构说明
By Renmian @ 2018.07.23

1. 此项目提供功能（截止今天）：
	a. 日志文件解析（spark）,入口类：com.gasq.bdp.task.logn.GuoanshequEdianLogParser
	b. 商品关联分析算法（Spark FPGrowth）,入口类：com.gasq.bdp.task.algorithms.FPGrowth4GasqV2
	c. 基于商品的用户商品推荐算法（Mahout版本）：
		c1. 用户推荐模型构造（MR）,入口类：com.gasq.bdp.task.algorithms.usermodel.*
		c2. 推荐算法,入口类：com.gasq.bdp.task.algorithms.mahout.GabdpBasedItemRecommendJob	
	d. 基于商品的门店推荐算法（Mahout版本）：复用推荐算法,入口类：com.gasq.bdp.task.algorithms.mahout.GabdpBasedItemRecommendJob
	e. 搜索商品分词分类算法（Spark）,入口类：com.gasq.bdp.task.anzlyzer.CreateClassTagIMonth
	f. VIPUserInfo数据处理（MR）,入口类：com.gasq.bdp.task.mr.ClearVIPUserInfoV2
	
2. 此项目合并了以下三个项目的内容：
	a.Hadoop-MR 项目功能（重写+迁移）
		1）日志解析 mrs.jar com.gasq.bdp.logn.Appliction 已经被 com.gasq.bdp.task.logn.GuoanshequEdianLogParser 替换；
		2）vip用户清洗处理 mrs.jar com.gasq.bdp.logn.mr.ClearVIPUserInfoV2 移至 com.gasq.bdp.task.mr.ClearVIPUserInfoV2
	b.spark-demo 项目功能（迁移）
		1）商品关联分析算法 com.gasq.bdp.logn.FPGrowth4GasqV2 移至  com.gasq.bdp.task.algorithms.FPGrowth4GasqV2;
		2）搜索商品分词分类 com.gasq.bdp.logn.anzlyzer.CreateClassTagIMonth 移至  com.gasq.bdp.task.anzlyzer.CreateClassTagIMonth
	c. Hadoop-algorithm 项目功能（迁移+重构）
		1）推荐算法  com.gasq.bdp.logn.mr.GabdpBasedItemRecommendJob 移至（重构）com.gasq.bdp.task.algorithms.mahout.GabdpBasedItemRecommendJob
		2）用户推荐模型构造 com.gasq.bdp.logn.mr.*/com.gasq.bdp.logn.model.* 移至 com.gasq.bdp.task.algorithms.usermodel.*
		3）关联分析MR算法 com.gasq.bdp.logn.AprioriStoreid/AprioriProvinceCode/AprioriCityCode/AprioriAdCode  未移植（考虑此算法已经用spark替代不再使用）
		
3. 项目编译，发布和部署说明
	a. 编译打包：进入${project_dir}执行命令：mvn clean package -DskipTests , 编译成功后会生成${project_dir}/target/gabdp-task.jar
	b. 部署目录：上传gabdp-task.jar到slave2主机的/usr/local/tomcat-8081/gabdp-task/ 目录下
	c. 运行任务可以参看t_sys_timer_job_exe_shell shell命令执行的调用

Add feature By Liyangyang @2019.01.28
1. 解析搜索日志数据index: guoanshequ-* / tags: "gsearch_analysis"
2. 并清洗入库存放到数据表default.t_search_log分区表中	