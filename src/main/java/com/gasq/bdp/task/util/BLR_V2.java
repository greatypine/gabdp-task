package com.gasq.bdp.task.util;

/***********************************************************
*  国 安 社 区 （北京) 科 技 有 限 公司
*  Class          : BLR
*  Description    : 二项逻辑回归模型（利用梯度下降类算法训练）
*  Author         : guoshaokun
*  Email          : guosk@gacs.citic
*  Date           : 2018.07.12
*  Version        : v0.1
*************************************************************/

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;

class lable_feat{
	String idSku;
	List<Double> vecVal;
	
	public lable_feat() {
		idSku = "";
		vecVal = new ArrayList<Double>();
	}
}



public class BLR_V2 {
	
	//parameter
	private int m_ifeaterNum;  // 特征数
	private double m_dAlpha;  //α
	private double m_dBeta;   //β
	private double m_dLambda1; //λ1   L1正则项 系数 （L1 1-范数）
	private double m_dLambda2; //λ2   L2正则项 系数  (L2 2-范数)   //用于FRTL算法

	private List<Double>  m_vTheta;
	private List<Double> m_vZ;     //用于FRTL算法
	private List<Double> m_vN;     //用于FRTL算法
	
	public void setFeaterNum(int num) {
		m_ifeaterNum = num;
	}
	public void setAlpha(double alpha) {
		m_dAlpha = alpha;
	}
	public void setBata(double beta) {
		m_dBeta = beta;
	}
	public void setLambda1(double lambda) {
		m_dLambda1 = lambda;
	}
	public void setLambda2(double lambda) {
		m_dLambda2 = lambda;
	}
	/*****************************************************************************
	 * 功能：算法参数初始化 函数
	 * 参数: alpha:学习因子，在FTRL算法中建议选择0.8；在其他梯度下降算法中建议选择0.1。
	 * 参数:bata :FTRL算法中特有参数，一般选择1就可以获得比较好的结果。
	 * 参数:lambda1:1-范数缩放因子建议选择0.1即可。
	 * 参数:lambda2:2-范数缩放因子，在这里只在FTRL算法中使用，建议选择0.1.
	 * 上述参数需根据实际的数据，择优选择；一般根据建议值就能够获得较好的分类结果。
	 ******************************************************************************/
	public void init(int featerNum, double alpha, double beta, double lambda1, double lambda2) {
		m_ifeaterNum = featerNum;
		m_dAlpha = alpha;
		m_dBeta = beta;
		m_dLambda1 = lambda1;
		m_dLambda2 = lambda2;
		
		m_vTheta = new ArrayList<Double>();
		m_vN     = new ArrayList<Double>();
		m_vZ     = new ArrayList<Double>();
		for(int i = 0; i < featerNum; i++) {
			m_vTheta.add(1.0);
			m_vN.add(0.0);
			m_vZ.add(0.0);
		}
	}
	
	/****************************************************************************
	 * 数据载入函数
	 * 参数：filename 数据文件，
	 * 参数：data 数据存储矩阵
	 * 参数：separator分割符
	 * 文件数据格式，文件中每行数据存储矩阵中的一行数据，元素之间使用分隔符分开。
	 * **************************************************************************/
	public void loadData(String filename, List<List<Double>> data,String separator) throws FileNotFoundException
	{
		File file = new File(filename);
		InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
		BufferedReader br = new BufferedReader(reader);
		String line ="";
		
		try {
			while((line = br.readLine()) != null)
			{
				List<Double> tmpList = new ArrayList<Double>();
				String strs[] = line.split(separator);
				for(int i = 1; i < strs.length; i++) {
					tmpList.add(Double.valueOf(strs[i]));
				}
				data.add(tmpList);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return;
	}

	/********************************************************
	 * 功能：S形曲线函数计算，形式为1.0/(1+exp(-wx) 
	 * 参数：data,输入的一个向量数据。
	 * 返回：该向量属于正例的概率值大小在区间[0.0--1.0]。
	 * ******************************************************/
	public double sigmoid(List<Double> data)
	{
		if (data.isEmpty())
			return 0.0;
		double tmp = 0.0;
		for (int j = 0; j < m_ifeaterNum; j++)
			tmp += m_vTheta.get(j)* data.get(j);
		return  1.0 / (1.0 + Math.exp(-tmp));
	}
	/****************************************************************
	 * 功能：类别预测函数
	 * 参数：data，输入的向量数据
	 * 参数：类别区分阈值，预备设置为0.5；对于特殊数据可根据实际侧重选择。
	 * 返回：1，大于阈值；0，小于阈值数据。1和0分别表示正例类别和负例类别
	 * ***************************************************************/
	public double predict(List<Double> data, double probablity)
	{
		double tmp = sigmoid(data);
		if (tmp > probablity)
			return tmp;
		return 0.0;
	}
	/*********************************
	 * 功能：训练模型的损失函数
	 * 参数：data 样本数据
	 * 返回：测试样本数据的损失值
	 * 备注: 在本程序中只在GD算法中使用该函数。
	 * *******************************/
	private double Loss(List<List<Double> > data)
	{
		if (data.isEmpty())
			return 0.0;

		double hvalue = 0.0;
		double lossvalue = 0.0;

		int row = (int)data.size();
		int col = (int)data.get(0).size();

		for (int i = 0; i < row; i++)
		{
			hvalue = sigmoid(data.get(i));
			if (hvalue < 1e-5)
				hvalue = 1e-5;

			if (hvalue > 1 - 1e-5)
				hvalue = 1 - 1e-5;

			lossvalue += -data.get(i).get(col -1) * Math.log(hvalue) - (1 - data.get(i).get(col -1))*Math.log(1 - hvalue);
		}
		lossvalue = lossvalue / row;

		return lossvalue;
	}
	
	/**************************************************************************
	 * 功能：模型训练函数；
	 * 参数：data，样本数据；
	 * 参数：maxIter 训练算法的最大迭代次数；
	 * 参数：algorithmType 算法选择参数，GD，传统梯度下降法；SGD，随机梯度下降算法；
	 * 		 SGDR带有1-范数正则的随机梯度下降法
	 * 		 MBGD，小批量梯度下降法，FTRL，FTRL算法
	 * 参数：batchNum针对于MBGD算法每次小批量的样本数量，一般选择batchNum=10
	 * ************************************************************************/
	public int trainModel(List<List<Double> > data, int  maxIter, String algorithmType, int batchNum)
	{
		
		if (data.isEmpty() || maxIter < 0 )
			return -1;
	
		int iter = 0;
		//增加截距项
		for(int i = 0; i < data.size(); i++)
		{
			data.get(i).add(0,1.0);
		}

		if (algorithmType == "GD")
		{
			GDalgorithm(data, maxIter);
		}
		else if (algorithmType == "SGD")
		{
			iter = 0;
			while (iter < maxIter)
			{
				for (int i = 0; i < data.size(); i++)
				{
					SGDalgorithm(data.get(i));
				}
				iter++;
			}
		}
		else if (algorithmType == "SGDR")
		{
			iter = 0;
			while (iter < maxIter)
			{
				for (int  i = 0; i < data.size(); i++)
				{
					SGDRalgorithm(data.get(i), iter);
				}
				iter++;
			}
		}
		else if (algorithmType == "MBGD")
		{
			iter = 0;
			while(iter < maxIter)
			{
				MBGDalgorithm(data, batchNum);
				iter++;
			}
			
		}
		else if(algorithmType == "FTRL")
		{
			iter = 0;
			while (iter < maxIter)
			{
				for (int  i = 0; i < data.size(); i++)
				{
					FTRLalgorithm(data.get(i));
				}
				iter++;
			}
		}
		return 0;
	}
	
	/*
	 * 功能：将模型数据载入程序
	 * 参数：filename 文件名称
	 * 参数：data数据载入变量
	 * 参数：separator 分隔符
	 * */
	public int loadWeights(String filename,String separator) throws FileNotFoundException {
		File file = new File(filename);
		InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
		BufferedReader br = new BufferedReader(reader);
		String line ="";
		
		if(!m_vTheta.isEmpty())
			m_vTheta.clear();
		m_vTheta = new ArrayList<Double>();
		
		try {
			while((line = br.readLine()) != null)
			{
				String strs[] = line.split(separator);
				for(int i = 1; i < strs.length; i++) {
					m_vTheta.add(Double.valueOf(strs[i]));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}	
		return 0;
	}
	/*
	 * 功能：将模型数据写入文件
	 * 参数：file文件名
	 * 参数：data 数据变量
	 * 参数：separator分割符
	 * */
	public int outputWeights(String file,String separator){
		
		File writename = new File(file); //文件路径
		try {
			writename.createNewFile();
			@SuppressWarnings("resource")
			BufferedWriter out = new BufferedWriter(new FileWriter(writename));
			for(int i = 0; i < m_vTheta.size()-1; i++) {
				out.write(m_vTheta.get(i).toString() + separator);
			}
			out.write(m_vTheta.get(m_vTheta.size()-1).toString()+"\r\n");
			
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		return 0;
	}
	
	/*梯度下降算法*/
	private int GDalgorithm(List<List<Double> >data,int  maxIter)
	{
		if (data.isEmpty())
			return -1;
		int row = (int)data.size();
		int col = (int)data.get(0).size() - 1;

		double  J_old = 0;
		double  J_new = Loss(data); // 计算J(θ)
		int iter = 0;
		double tmp = 0.0;
		while (iter < maxIter && Math.abs(J_new - J_old) > 1e-5)
		{
			J_old = J_new;

			for (int j = 0; j < col; j++)
			{
				tmp = 0.0;
				for (int i = 0; i < row; i++)
				{
					tmp += (sigmoid(data.get(i)) - data.get(i).get(col)) * data.get(i).get(j);
				}

				if (j>0)
				{
					double element = m_vTheta.get(j) - m_dAlpha * (tmp / row + m_dLambda1 * m_vTheta.get(j) / row);
					m_vTheta.set(j, element);
				}
				else
				{
					double element = m_vTheta.get(j) - m_dAlpha * tmp/row;
					m_vTheta.set(j, element);
				}
			}
			J_new = Loss(data);
			iter++;
		}
		return 0;
	}
	
	/*随机梯度下降算法*/
	private int  SGDalgorithm(List<Double> data)
	{
		if (data.isEmpty())
			return -1;
		double err, grad;
		err = sigmoid(data) - data.get(data.size() -1);

		for (int j = 0; j < m_ifeaterNum; j++)
		{
			grad = err * data.get(j);
			
			grad = m_vTheta.get(j) - m_dAlpha * grad;
			m_vTheta.set(j, grad);
		}

		return 0;
	}
	
	/*带有1-范数正则的随机梯度下降算法*/
	private int SGDRalgorithm(List<Double> data, int iternum)
	{
		if (data.isEmpty())
			return -1;
		double err, grad;
		err = sigmoid(data) - data.get(data.size() -1);

		for (int j = 0; j < data.size()-1; j++)
		{
			grad = err * data.get(j);
			if (j > 0)
			{
				double element = m_vTheta.get(j) - m_dAlpha / (Math.exp(j + iternum)) * (grad + m_dLambda1 * m_vTheta.get(j));
				m_vTheta.set(j, element);
			}
			else
			{
				double element = m_vTheta.get(j) - m_dAlpha / (Math.exp(j + iternum)) * grad;
				m_vTheta.set(j, element);
			}
		}

		return 0;
	}
	
	/*FTRL算法*/
	private int FTRLalgorithm(List<Double> data)
	{
		if (data.isEmpty())
			return -1;
		double grad= 0.0,deta = 0.0, gradS = 0.0, err = 0.0;

		err = sigmoid(data) - data.get(data.size()-1);

		for (int j = 0; j < data.size()-1; j++)
		{

			//更新迭代 m_vZ, m_vN 
			grad = err * data.get(j);
			gradS = grad *grad;
			deta = 1.0 / m_dAlpha *(Math.sqrt(m_vN.get(j) + gradS) - Math.sqrt(m_vN.get(j)));
			m_vZ.set(j, m_vZ.get(j) + grad - deta * m_vTheta.get(j));
			m_vN.set(j, m_vN.get(j) + gradS);

			// 更新迭代m_vTheta;
			if (Math.abs(m_vZ.get(j)) < m_dLambda1)
			{
				m_vTheta.set(j, 0.0);
			}
			else
			{
				double element = -1.0 / ((m_dBeta + Math.sqrt(m_vN.get(j))) / m_dAlpha + m_dLambda2) * (m_vZ.get(j) - m_dLambda1 * m_vZ.get(j) / Math.abs(m_vZ.get(j)));
				m_vTheta.set(j, element);
			}
		}
		return 0;
	}
	
	/*小批量梯度下降算法*/
	private int MBGDalgorithm(List<List<Double> > data, int batchNum)
	{
		if (data.isEmpty())
			return -1;
		
		Collections.shuffle(data);
		
		Set<Integer> setN = new TreeSet<>();
		Random r = new Random();
		for (int i = 0; i < data.size(); i++)
		{
			int num = r.nextInt(data.size());
			if(setN.contains(num))
			{
				continue;
			}
			setN.add(num);
			
			if (batchNum == setN.size())
				break;
		}
		int col = data.get(0).size()-1;
		double tmp = 0.0;

		for (int j = 0; j < col; j++)
		{
			tmp = 0.0;
			for (int i = 0; i <batchNum ; i++)
			{
				tmp += (sigmoid(data.get(i)) - data.get(i).get(col)) * data.get(i).get(j);
			}

			if (j>0)
			{
				double element = m_vTheta.get(j) - m_dAlpha * (tmp / batchNum + m_dLambda1 * m_vTheta.get(j) / batchNum);
				m_vTheta.set(j, element);
			}
			else
			{
				double element = m_vTheta.get(j) - m_dAlpha * tmp/batchNum;
				m_vTheta.set(j, element);
			}
		}

		return 0;
	}
	
	
	public int  GeneralSample(List<List<Double>> data, int multiple) {
		if(data.isEmpty())
			return -1;
		int num = data.get(0).size() -1;
		
	 //正负样本个数
		List<List<Double>> oneData = new ArrayList<List<Double>>();
		List<List<Double>> zeroData = new ArrayList<List<Double>>();
		
		for(int i = 0; i < data.size(); i++)
		{
			if(data.get(i).get(num) == 1) {
				oneData.add(data.get(i));
			}
			else {
				zeroData.add(data.get(i));
			}
		}
		
		int oneSize = oneData.size();
		int zeroSize = zeroData.size();
		
		//---防止分母为零----
		
		if(oneSize <= 0 ) {
			//写日志 --
			//do...
			
			return -2;
		}

		Set<Integer> setNum = new TreeSet<>();
		int rate = zeroSize / oneSize;
		int extractNum = 0;
		int tmpnum = 0;
		
		if(rate > 8) {
			
			data = new ArrayList<List<Double>> ();
			extractNum = multiple * oneSize;
			for(int i = 0; i < zeroSize; i++) {
				tmpnum = (int) (Math.random() * zeroSize);
				if(setNum.contains(tmpnum))
					continue;
				setNum.add(tmpnum);
				
				if (extractNum == setNum.size())
					break;
			}
			
			
			//按照新的比例生成样本数据
			for(Integer num1 : setNum) {
				data.add(zeroData.get(num1));
			}
			for(int i = 0; i < oneData.size(); i ++) {
				data.add(oneData.get(i));
			}

		}
		// 数据乱序处理
		Collections.shuffle(data);
		Collections.shuffle(data);
		
		return 0;
		
	}
	
	public int DataPreprocess(List<List<Double>> data) {
		
		if(data.isEmpty())
			return -1;
		
		int row = data.size();
		int col = data.get(0).size()-1;
		
		//临时变量
		List<Double> maxValue = new ArrayList<Double>();
		List<Double> minValue = new ArrayList<Double>();
		
		for(int i = 0; i < col; i++) {
			maxValue.add(Double.MIN_VALUE);
			minValue.add(Double.MAX_VALUE);
		}
		
		//数据对数平滑处理
		double tmp = 0;
		for(int i = 0; i < row;i++) {
			for(int j = 0; j < col; j++) {
				tmp = Math.log(1+ data.get(i).get(j));
				if(tmp <minValue.get(j))
					minValue.set(j, tmp);
				if(tmp > maxValue.get(j))
					maxValue.set(j, tmp);
				data.get(i).set(j, tmp);
			}
		}
		//数据规范化处理
		
		for(int i = 0; i < row; i++) {
			for(int j = 0; j < col; j++) {
				tmp = maxValue.get(j) - minValue.get(j);
				if(tmp > 0) {
					data.get(i).set(j, (data.get(i).get(j) - minValue.get(j))/tmp);
				}
			}
		}
		
		// 正式环境下降每个特征维度的最大最小值写入配置文件
		//do.......
		
		return 0;	
	}
	
	
	// 测试用函数函数，抽取样本数据和测试数据
	public int ExtractSample(List<List<Double> > Data, List<List<Double> > sampleData, List<List<Double>> testData, double rate){
		
		if(Data.isEmpty())
			return -1;
		if(rate < 0)
			return -2;
		
		if(sampleData == null)
		{
			sampleData = new ArrayList<List<Double>>();
		}
		if(testData == null)
		{
			testData  = new ArrayList<List<Double>>();
		}
		
		//随机打乱处理
		Collections.shuffle(Data);
		Random r = new Random();
		//抽取训练数据和测试数据
		Set<Integer> test = new TreeSet<>();
		int total = Data.size();
		int testNum = (int) (total * rate);
		int num = 0;
		if(Math.abs(1.0-rate)>1e-5) {
			
			for(int i = 0; i < total; i++) {
				num = r.nextInt(total);
				if(test.contains(num))
				{
					continue;
				}
				test.add(num);
				testData.add(Data.get(num));
				if(testNum == test.size())
					break;
			}
			for(int i = 0; i <total; i++) {
				if(test.contains(i)) {
					continue;
				}
				sampleData.add(Data.get(i));	
			}
		}
		return 0;
	}
	
	/*测试函数*/
	public double Test(List<List<Double>> testData, double probability) {
		
		if(testData.isEmpty())
			return 0.0;
		
		double accuracy = AccuracyTest(testData,probability);
		
		return accuracy;
	}
	
	/*算法精度测定函数*/
	public double AccuracyTest(List<List<Double>> testData, double probability) {
		
		if(testData.isEmpty())
			return 0.0;
		
		//--增加截距项--
		for(int i = 0; i <testData.size(); i++) {
			testData.get(i).add(0, 1.0);
		}
		
		
		double num = 0;
		int count = 0;
		//double tmp = 0.0;
		for(int i = 0; i <testData.size(); i++) {
			num = predict(testData.get(i), probability);
			if(num == testData.get(i).get(testData.get(i).size() -1).intValue()){
				count++;
			}
		}
		return (double)count/testData.size();
	}
	

	public void loadPreData(String filename, List<lable_feat> data, String separator) throws FileNotFoundException {
		
		File file = new File(filename);
		InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
		BufferedReader br = new BufferedReader(reader);
		String line ="";
		
		try {
			while((line = br.readLine()) != null)
			{
				lable_feat tmp = new lable_feat();
				//List<Double> tmpList = new ArrayList<Double>();
				String strs[] = line.split(separator);
				tmp.idSku = strs[0];
				for(int i = 1; i < strs.length; i++) {
					tmp.vecVal.add(Double.valueOf(strs[i]));
				}
				data.add(tmp);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return;
	}
	public List<Double> getM_vTheta() {
		return m_vTheta;
	}
	/*算法输出结果*/
	public List<String> trainOutResult(Map<String,String> baseKVData, List<List<Double>> datas1, double probability) throws Exception {
		if(datas1.isEmpty())
			return null;
		List<String> result = new ArrayList<>();
		if(datas1.size()>0) {
			for (List<Double> list : datas1) {
				m_ifeaterNum = datas1.size();
				String key = StringUtils.join(list, "-");
//				key = key.substring(4,key.length()).trim();
				String md5 = CommonUtils.md5(key);
				String v = baseKVData.get(md5);
				double nummap = predict(list, probability);
				if(nummap > 0) {
					String vs = String.join("-",v,nummap+"");
					vs = vs.replace("-", "\t");
					result.add(vs);
				}
			}
		}
		return result;
	}
}