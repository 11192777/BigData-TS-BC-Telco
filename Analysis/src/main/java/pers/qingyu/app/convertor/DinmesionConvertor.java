package pers.qingyu.app.convertor;

import pers.qingyu.app.base.BaseDemension;
import pers.qingyu.app.util.LRUCache;

//获取联系人&时间维度ID
public class DinmesionConvertor {

	private LRUCache keyCache = new LRUCache(1000);

	public int getDimensionId(BaseDemension demension){

		//0.获取缓存中的值
		Integer id = keyCache.get("");

		//1.查询sql表相应维度是否有值

		//2.如果查询结果为空，执行插入操作

		//3.再次执行查询

		//4.将结果写入缓存

		//5.将查询结果返回


		return 0;
	}
}
