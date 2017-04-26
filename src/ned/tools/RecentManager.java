package ned.tools;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import ned.types.Document;
import ned.types.GlobalData;
import ned.types.LRUCache;

public class RecentManager {
	
	private int recentSize;
	
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
		recentCopy=new ArrayList<String>();

	}
	
	public List<String> getRecentCopy(){
		
			return recentCopy;
		
	}
	public int  getRecentsize(){
		return recent.size();
		
	}
	public void  AddToRecent(String docId){
		synchronized(recentBuffer){
			recentBuffer.add(docId);
			if(recentBuffer.size()>=this.recentBufferSize){
				fulshBuffer();
			}
			
		}
		
		
		
		
	}
	 private void fulshBuffer(){
			waitOnRecent();
			locked=true;
			synchronized(recent){
				for(String str:recentBuffer){
						recent.add(str);
						if(recent.size()>recentSize) recent.remove(0);
					}				
			}
			synchronized(recentBuffer){
				recentBuffer=new ArrayList<String>();
			}
			
			
		locked=false;
		prepareCopy();
	}

	public List<String> getRecent() {
		
		return recent;
	}
	private void prepareCopy(){
		
			ArrayList<String> temp = new ArrayList<String>();
			waitOnRecent();
			locked=true;
			for(String str:recent){
				if(str!=null) 
					temp.add(str);	
			}
			
			locked=false;
			copyRecent=false;
			 recentCopy=temp;
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
