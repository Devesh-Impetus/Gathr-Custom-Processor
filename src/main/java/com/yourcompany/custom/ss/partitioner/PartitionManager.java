package com.yourcompany.custom.ss.partitioner;

import com.yourcompany.component.ss.common.Constants;

public class PartitionManager {
	
	public static  String getTableName (Integer TimeSpan) throws Exception
	{
		
		int configureTime = Integer.parseInt(Constants.CONFIGURE_TIME);
		String tableName = null;
		
		    			Integer starttime=null;
		    			Integer Temp = TimeSpan;
		    			Temp=Temp/(Integer.parseInt(Constants.CONFIGURE_TIME));
		    			Temp = Temp*(Integer.parseInt(Constants.CONFIGURE_TIME));
		    			starttime=Temp;
		    			Temp =Temp+(Integer.parseInt(Constants.CONFIGURE_TIME));
		    			tableName="_"+starttime+"_"+Temp;
		    			
		if(tableName==null)
		{
//			log.error("The input Timesspan is "+ TimeSpan);
//			log.error("cant Calculate table Name");
			throw new Exception("cant Calculate table Name");
		}
		return tableName;
	}

}
