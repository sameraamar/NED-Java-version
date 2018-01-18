package ned.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.util.Hashtable;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import ned.types.Document;

public class FilterCorset {
	private Map<String, Double> include = new Hashtable<String, Double>();
	private int counter;
	private int fileidx;
	private String FOLDER = "c:/data/Thesis/events_db/petrovic";

	public static void main(String[] args) throws IOException {
		FilterCorset f = new FilterCorset();
		
		f.readCorset();
		f.main();
	}
	
	public void main() throws IOException {
		
		int c = 0;
		counter = 0;
		String outputFile = "C:\\Users\\t-samera\\Downloads\\corset.json";

		Writer out = new FileWriter(outputFile);
		BufferedWriter encoder = new BufferedWriter( out ) ;

		while (c < 30_000_000)
		{
			if (buffered==null || !buffered.ready())
			{
				fileidx++;
				openFile();
			}
			
			String line = buffered.readLine();
			String sourceFile = gzfiles[fileidx];
			if (line == null)
				continue;
			Document doc = Document.parse(line, true, sourceFile);
			
			String id = doc.getId();
			
			id = handleLine(encoder, line, id);
			c++;
			
			if(c % 50000 == 0)
				System.out.println("processed " + c + " found " + counter );
		}
		
		encoder.close();
		
		System.out.println("processed " + c + " found " + counter );
	}

	private String handleLine(BufferedWriter encoder, String line, String id) throws IOException {
		if(id.equals("90113992328093696"))
			id = "90113992328093696";
		
		if(include.containsKey( id ))
		{
			counter++;
			String created_at = ", \"created_at\"";
			int i = line.indexOf(created_at);
			if (i<0)
				System.out.println("Very strange line: " + line);
			Double score = include.get(id);
			line = line.replaceFirst(created_at, ", \"score\": " + score.toString() + created_at);
			encoder.write(line + "\n");
		}
		return id;
	}

	public FilterCorset()
	{
		fileidx = -1;
		counter = 0;
	}

	protected int hasNextHook() throws Exception {
		if( buffered == null )
			return -1;
		
		boolean isReady = buffered.ready();
		if( !isReady )
		{
			fileidx++;
			if(!openFile())
				return 0;
		}
		
		return 1;
	}

	private void readCorset()
	{
		String[] filenames = {"C:\\Users\\t-samera\\Downloads\\coresets_500000_2.csv", 
							  "C:\\Users\\t-samera\\Downloads\\coresets_500000_1.csv"};


		try {
			
			for(String filename : filenames)	
			{
				System.out.println("process file: " + filename);
				Writer out = new FileWriter(filename + ".txt");
				BufferedWriter encoder = new BufferedWriter( out ) ;
				
				
				Reader stream = new FileReader(filename);
				BufferedReader decoder = new BufferedReader( stream ) ;
				
				
				while (decoder.ready())
				{
					String line = decoder.readLine().trim();
					if (line.length()==0)
						continue;
					
					String[] temp = line.split(",");
					
					Long d = (long) Double.parseDouble(temp[0].trim());
					//Long i = Long.parseLong(temp[0].trim());
					String id = d.toString();
					Double score = Double.parseDouble(temp[1].trim());
					
					include.put(id, score + include.getOrDefault(id, 0.0));
					
					out.write(id + "," + score.toString() + "\n");
				}
				
				decoder.close();
				encoder.close();
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}
		
	}


	private boolean openFile() throws IOException 
	{
		GZIPInputStream stream;

		if(fileidx >= gzfiles.length)
			return false;
		
		System.out.println("opening file (" + fileidx + "): " + gzfiles[fileidx]);
		stream = new GZIPInputStream(new FileInputStream(FOLDER + "/" + gzfiles[fileidx]));

		Reader decoder = new InputStreamReader(stream, "UTF-8");
		BufferedReader temp = buffered;
		buffered = new BufferedReader(decoder);
		if (temp != null)
			temp.close();
		
		return true;
	}
	String[] gzfiles2 = {"petrovic_00000000.gz",
            "petrovic_00500000.gz"};

	String[] gzfiles = {"petrovic_00000000.gz",
                    "petrovic_00500000.gz",
                    "petrovic_01000000.gz",
                    "petrovic_01500000.gz",
                    "petrovic_02000000.gz",
                    "petrovic_02500000.gz",
                    "petrovic_03000000.gz",
                    "petrovic_03500000.gz",
                    "petrovic_04000000.gz",
                    "petrovic_04500000.gz",
                    "petrovic_05000000.gz",
                    "petrovic_05500000.gz",
                    "petrovic_06000000.gz",
                    "petrovic_06500000.gz",
                    "petrovic_07000000.gz",
                    "petrovic_07500000.gz",
                    "petrovic_08000000.gz",
                    "petrovic_08500000.gz",
                    "petrovic_09000000.gz",
                    "petrovic_09500000.gz",
                    "petrovic_10000000.gz",
                    "petrovic_10500000.gz",
                    "petrovic_11000000.gz",
                    "petrovic_11500000.gz",
                    "petrovic_12000000.gz",
                    "petrovic_12500000.gz",
                    "petrovic_13000000.gz",
                    "petrovic_13500000.gz",
                    "petrovic_14000000.gz",
                    "petrovic_14500000.gz",
                    "petrovic_15000000.gz",
                    "petrovic_15500000.gz",
                    "petrovic_16000000.gz",
                    "petrovic_16500000.gz",
                    "petrovic_17000000.gz",
                    "petrovic_17500000.gz",
                    "petrovic_18000000.gz",
                    "petrovic_18500000.gz",
                    "petrovic_19000000.gz",
                    "petrovic_19500000.gz",
                    "petrovic_20000000.gz",
                    "petrovic_20500000.gz",
                    "petrovic_21000000.gz",
                    "petrovic_21500000.gz",
                    "petrovic_22000000.gz",
                    "petrovic_22500000.gz",
                    "petrovic_23000000.gz",
                    "petrovic_23500000.gz",
                    "petrovic_24000000.gz",
                    "petrovic_24500000.gz",
                    "petrovic_25000000.gz",
                    "petrovic_25500000.gz",
                    "petrovic_26000000.gz",
                    "petrovic_26500000.gz",
                    "petrovic_27000000.gz",
                    "petrovic_27500000.gz",
                    "petrovic_28000000.gz",
                    "petrovic_28500000.gz",
                    "petrovic_29000000.gz",
                    "petrovic_29500000.gz"  
                   };
	private BufferedReader buffered;

	
}
