package ned.tools;

import java.util.concurrent.ExecutorService;

import ned.main.ExecutorMonitorThread;

public class FlattenDatasetMonitor extends ExecutorMonitorThread {

	public FlattenDatasetMonitor(ExecutorService executorService, int delay) {
		super(executorService, delay);
	}
	
	@Override
	protected void printHook() {
		StringBuffer msg = new StringBuffer();
		
		String id = "86383670600019968";
		msg.append("\tid2group[").append(id).append("]: ");
		
		Entry entry = FlattenDatasetMain.id2group.get( id );
		if(entry == null)
			msg.append("null");
		else
		{
			msg.append(entry.leadId).append(",");
			msg.append(entry.level);
		}
		
		System.out.println(msg.toString());
	}
	
}
