package ned.tools;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import ned.types.Document;
import ned.types.LRUCache;

public class RecentManager {
	
	private int recentSize=2000;
	
	private boolean copyRecent=true;
	private int recentBufferSize=20;
	private List <String> recent;
	private List <String> recentCopy;
	
	private List <String> recentBuffer;
	private boolean locked=false;
	public RecentManager(int recentSize ){
		this.recentSize=recentSize;
		this.recentBufferSize=recentSize/10;
		recentBuffer=new ArrayList<String>();
		recent=new ArrayList<String>();

	}
	
	public List<String> getRecentCopy(){
		if(recentCopy!=null && !recentCopy.isEmpty() && !copyRecent){
			return recentCopy;
		}
		recentCopy=new ArrayList<String>();
		waitOnRecent();
		locked=true;
		for(String str:recent){
			if(str!=null) 
			recentCopy.add(str);	
		}
		
		locked=false;
		copyRecent=false;
		return recentCopy;
		
	}
	public int  getRecentsize(){
		return recent.size();
		
	}
	public void  AddToRecent(String docId){
		
		recentBuffer.add(docId);
		if(recentBuffer.size()>=this.recentBufferSize){
			fulshBuffer();
		}
		
		
	}
	 private void fulshBuffer(){
			waitOnRecent();
			locked=true;
			synchronized(recent){
				synchronized(recentBuffer){
					for(String str:recentBuffer){
						recent.add(str);
						if(recent.size()>recentSize) recent.remove(0);
					}
				}
			}
			
			recentBuffer=new ArrayList<String>();
		locked=false;
		copyRecent=true;
	}

	public List<String> getRecent() {
		
		return recent;
	}
	
	private void  waitOnRecent(){
		while(locked){
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return ;
		
	}
	
	


}
