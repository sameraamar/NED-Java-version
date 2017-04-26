package ned.tools;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import ned.types.Document;
import ned.types.GlobalData;
import ned.types.LRUCache;

public class RecentManager {
	
	private int recentSize;
	
	//private boolean copyRecent=true;
	private int recentBufferSize=20;
	private List <String> recent;
	private List <String> recentCopy;
	private int recentActualSize;
	private List <String> recentBuffer;
	private boolean locked=false;
	public RecentManager(int recentSize ){
		recentActualSize=0;
		this.recentSize=recentSize;
		this.recentBufferSize=recentSize/20;
		recentBuffer= new ArrayList<String>();
		recent=new ArrayList<String>();
		recentCopy=new ArrayList<String>();
	}	
	public List<String> getRecentCopy(){		
		return recentCopy;		
	}
	public int  getRecentsize(){
		return recentActualSize;		
	}
	public void  AddToRecent(String docId){
		//System.out.println("Start AddToRecent");
		long start=System.currentTimeMillis();
		recentBuffer.add(docId);		
		if(recentBuffer.size()>=this.recentBufferSize){
			Runnable task = () -> {
				fulshBuffer();				
			};		
			try {
				ExecutionHelper.asyncRun(task);				
			} catch (Exception e) {				
				e.printStackTrace();
			}			
		}
		long dtime=System.currentTimeMillis()-start;
		if(dtime>2)
			System.out.println("End AddToRecent time="+dtime);
	}
	private void fulshBuffer(){
		long start=System.currentTimeMillis();
		waitOnRecent();
		locked=true;			
		recent.addAll(recentBuffer);
		recentBuffer.clear();
		//synchronized(recent){
			if(recent.size()>recentSize){
				int startIndex=recent.size()-recentSize;
				recent=recent.subList(startIndex, recent.size());
				recentActualSize=recent.size();
			}
		//}
			long dtime=System.currentTimeMillis()-start;
			if(dtime>5) System.out.println("End fulshBuffer time="+dtime);
		locked=false;	
		prepareCopy();		
	}
	private void prepareCopy(){	
		long start=System.currentTimeMillis();
		List<String> temp = new ArrayList<String>();
		waitOnRecent();
		locked=true;	
		int length=recent.size()-1;		
		for(int i=length;i>=0;i--){
			temp.add(recent.get(i));
		}
		locked=false;	
		recentCopy=temp;	
		long dtime=System.currentTimeMillis()-start;
		if(dtime>5) System.out.println("End prepareCopy time="+dtime);

	}	
	private void  waitOnRecent(){
		while(locked){
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return ;		
	}
	

}
