package cn.edu.bit.jingang;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import kba.*;

/**
 * @author Jingang Wang
 *2013-3-9
 */
public class StreamCorpus {
    
	//given a xz compressed file, return the ArrayList of StreamItems
    public ArrayList<StreamItem> readStremItemFromFile(String filepath) throws IOException, TException
    {
    	File file = new File(filepath);
    	InputStream input = new FileInputStream(file);    	
    	XZCompressorInputStream stream = new XZCompressorInputStream(input); 	
    	TTransport transport;
    	transport = new TIOStreamTransport(stream);
    	transport.open();  	
    	TProtocol protocol = new TBinaryProtocol(transport);
    	ArrayList<StreamItem> doc_list = new ArrayList<StreamItem>();
    	int num_docs = 0;
    	while(true)
    	{
    		StreamItem doc = new StreamItem(); 
            try { 
                doc.read(protocol); 
            } catch (TTransportException e) { 
                if (e.getType() == TTransportException.END_OF_FILE) 
                { 
                    break; 
                } 
            } 
            doc_list.add(doc);
            num_docs += 1;
    	}
    	System.out.println(file.getName() + " contains " + num_docs + " documents.");
    	transport.close();
    	return doc_list;
    }


	public static void main(String[] args) {
		StreamCorpus streamcorpus = new StreamCorpus();
		String filepath = "\2011-11-24-00\news.291ee802fa9c2485cc5f1d5d535bf14a.xz";
		try{
			streamcorpus.readStremItemFromFile(filepath);
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
			
	}

}