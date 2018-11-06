package com.gasq.bdp.task.anzlyzer;

import java.util.List;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

public class JeAnalyzer {
 
	public static void test() {
        //只关注这些词性的词
//        Set<String> expectedNature = new HashSet<String>() {{
//            add("n");add("v");add("vd");add("vn");add("vf");
//            add("vx");add("vi");add("vl");add("vg");
//            add("nt");add("nz");add("nw");add("nl");
//            add("ng");add("userDefine");add("wh");
//        }};
        String pattern = "[(.0-9)]+ml|\\【.*?\\】|/|[.0-9]+cm|[.0-9]+g|[.0-9]+G|[.0-9]+kg|[.0-9]+KG|[.0-9]+L|[.0-9]+箱|[.0-9]+平米|[.0-9]+天|[.0-9]+月|[.0-9]+日|嘻嘻哈哈\r\n" + 
        		"          |[.0-9]+袋|[.0-9]+卷|[.0-9]+组|[.0-9]+斤|[.0-9]+片|[.0-9]+套|[.0-9]+瓶|[.0-9]+份|[.0-9]+包|[.0-9]+套|[.0-9]+毫升|[.0-9]+ML|[.0-9]+支|[.0-9]+寸|嘻嘻哈哈\r\n" + 
        		"          |[.0-9]+英寸|[.0-9]+罐|[.0-9]+层|[.0-9]+张|[.0-9]+块|[.0-9]+mm|[.0-9]+枚|[.0-9]+个|[.0-9]+盒|[.0-9]+节|[.0-9]+粒|\\/袋|\\/组|嘻嘻哈哈\r\n" + 
        		"          |\\?|新品低至|五折|\\\\（.*?\\）|\\+|\\-|买一赠一|保税发货|组合|起发|积分商城|\\*+[0-9][0-9]|集采|\\*|嘻嘻哈哈\r\n" + 
        		"          |买一送一|请联系客服挑选|根据实际价格下单结算|请联系客服根据实际价格下单|根据实际金额下单|团购|随机赠送|首单体验|嘻嘻哈哈\r\n" + 
        		"          |当天下午三点前下单次日送达|订单备注颜色|备注需求颜色|订单备注颜色|北大荒|禾煜|惊爆价|套装|科控专享|门店活动下单专用|白领厨房|嘻嘻哈哈\r\n" + 
        		"          |[.0-9]+克|[.0-9]+元|[.0-9]+支装|\\(.*?\\)|嘻嘻哈哈\r\n" + 
        		"          \",\"|\\【|\\】|\\（|\\）|\\(|\\)|\\*|\\#|\\:|\\、|\"|\\,|\\.|\\!|\\_";
        String str = "欢迎使用ansj_seg,(ansj中文分词)在这里如果你遇到什么问题都可以联系我.我一定尽我所能.帮助大家.ansj_seg更快,更准,更自由!菜籽油10kg,酸奶8箱,小米1袋" ;
        String newstr = str.replaceAll(pattern, "");
        Result result = ToAnalysis.parse(newstr); //分词结果的一个封装，主要是一个List<Term>的terms
//        System.out.println(result.getTerms());

        List<Term> terms = result.getTerms(); //拿到terms
//        System.out.println(terms.size());

        for(int i=0; i<terms.size(); i++) {
            String word = terms.get(i).getName(); //拿到词
            System.out.println(word);
//            String natureStr = terms.get(i).getNatureStr(); //拿到词性
//            if(expectedNature.contains(natureStr)) {
//                System.out.println(word + ":" + natureStr);
//            }
        }
    }

    public static void main(String[] args) {
        test();
    }
}