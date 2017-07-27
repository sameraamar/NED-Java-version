package ned.tools.s3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
//import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;


public class AWSS3Helper {
	private static String bucketName = "magnet-fwm";
	private static String prefix = "twitter";
	private List<S3ObjectSummary> files;
	private int nextfileIndex=0;	
	private AmazonS3 s3Client ;
	public AWSS3Helper(){
		AWSCredentials credentials = new BasicAWSCredentials("access_key", "access_secret");
		ClientConfiguration configuration = new ClientConfiguration();
		configuration.setMaxErrorRetry(3);
		configuration.setConnectionTimeout(501000);
		configuration.setSocketTimeout(501000);
		configuration.setProtocol(Protocol.HTTP);		
        s3Client = new AmazonS3Client(credentials,configuration);
        loadFileList();
        getNextFile();
	}
	private void loadFileList(){
		
		if (s3Client.doesBucketExist(bucketName)) {
			 System.out.format("Bucket %s already exists.\n", bucketName);
			 ObjectListing listing = s3Client.listObjects( bucketName, prefix );
			 List<S3ObjectSummary> summaries = listing.getObjectSummaries();
			 files=new ArrayList();
			 while (listing.isTruncated()) {
			    listing = s3Client.listNextBatchOfObjects (listing);
			    files.addAll (listing.getObjectSummaries());			    
			 }
			
			 
		}else{
			System.out.format("Bucket %s doest Not exists.\n", bucketName);
		}
		
		
	}
	public InputStream getNextFile(){
		
		InputStream result = s3Client.getObject(files.get(nextfileIndex).getBucketName(), files.get(nextfileIndex).getKey()).getObjectContent() ;
		nextfileIndex++;
		return result;
		
	}
	public  static void main(String[] args) throws IOException {
		
        try {
            System.out.println("Downloading an object");
         new AWSS3Helper();
           
            
           // Get a range of bytes from an object.
            /*
            GetObjectRequest rangeObjectRequest = new GetObjectRequest(
            		bucketName, key);
            rangeObjectRequest.setRange(0, 10);
            S3Object objectPortion = s3Client.getObject(rangeObjectRequest);
            
            System.out.println("Printing bytes retrieved.");
            displayTextInputStream(objectPortion.getObjectContent());
            */
            
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which" +
            		" means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means"+
            		" the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    private static void displayTextInputStream(InputStream input)
    throws IOException {
    	// Read one text line at a time and display.
        BufferedReader reader = new BufferedReader(new 
        		InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }

}
