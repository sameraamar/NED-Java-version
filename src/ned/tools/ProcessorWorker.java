package ned.tools;

abstract public class ProcessorWorker implements Runnable {

	protected String docJson;

	public ProcessorWorker(String docJson) {
		super();
        this.docJson = docJson;
	}

	@Override
	public void run() {
	    processCommand();
	}

	abstract protected void processCommand() ;

}