package ned.tools;
import java.util.ArrayList;
import java.util.List;

import ned.types.Document;
import ned.types.LRUCache;

public class RecentManager {
	
	private int recentSize=2000;

	private int recentBufferSize=20;
private List <String> recent;
	private List <String> recentBuffer;
	public RecentManager(int recentSize ){
		this.recentSize=recentSize;
		this.recentBufferSize=recentSize/100;
		recentBuffer=new ArrayList<String>();
		recent=new ArrayList<String>();

	}
	public List <String> getRecent(){
		return recent;
		
	}
	public int  getRecentsize(){
		return recent.size();
		
	}
	public void  AddToRecent(String docId){
		synchronized(recentBuffer){
		recentBuffer.add(docId);
		if(recentBuffer.size()>=this.recentBufferSize){
			Runnable task=()->{
				fulshBuffer();
			};
		ExecutionHelper.asyncRun(task);
		}
		}
		
	}
	synchronized private void fulshBuffer(){
			
			recent.addAll(recentBuffer);
			if(recent.size()>=recentSize){
				recent=recent.subList(0, recentBuffer.size());

			}
			recentBuffer=new ArrayList<String>();
		
	}
	
	


}
