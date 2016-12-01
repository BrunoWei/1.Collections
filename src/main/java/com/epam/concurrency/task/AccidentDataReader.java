package com.epam.concurrency.task;


import com.epam.data.RoadAccident;
import com.epam.data.RoadAccidentBuilder;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class AccidentDataReader implements Runnable{

    private Integer batchSize;
    private String dataFileName;
    private int batchCount;
    private int recordCount;
    private boolean hasFinished = false;
    private RoadAccidentCsvParser roadAccidentParser  = new RoadAccidentCsvParser();

    private Iterator<CSVRecord> recordIterator;
    private Logger log = LoggerFactory.getLogger(AccidentDataReader.class);
    
    protected BlockingQueue<List<RoadAccident>> roadAccidentBlockingQueue = null;

    public AccidentDataReader(BlockingQueue<List<RoadAccident>> roadAccidentBlockingQueue) {
		super();
		this.roadAccidentBlockingQueue = roadAccidentBlockingQueue;
	}

	public void init(int batchSize, String dataFileName){
        this.batchSize = batchSize;
        this.dataFileName = dataFileName;
        batchCount = 0;
        recordCount = 0;
        hasFinished = false;
        prepareIterator();
    }

    public void reset(int batchSize,  String dataFileName){
        init(batchSize, dataFileName);
    }

    private void prepareIterator(){
        try{
            Reader reader = new FileReader(dataFileName);
            recordIterator = new CSVParser(reader, CSVFormat.EXCEL.withHeader()).iterator();
        }catch (Exception e){
            log.error("Failed to prepare file iterator for  file : {}", dataFileName, e);
            throw new RuntimeException("Failed to prepare file iterator for  file : " + dataFileName, e);
        }
    }

    public List<RoadAccident> getNextBatch(){
        List<RoadAccident> roadAccidentBatch = new ArrayList<RoadAccident>();
        int recordCountInCurrBatch = 0;
        RoadAccident roadAccidentItem = null;
        while(recordCountInCurrBatch < batchSize && recordIterator.hasNext() ){
            roadAccidentItem = roadAccidentParser.parseRecord(recordIterator.next());
            if(roadAccidentItem != null){
                roadAccidentBatch.add(roadAccidentItem);
                recordCountInCurrBatch++;
            }
        }

        if(recordCountInCurrBatch != 0){
            ++batchCount;
            recordCount = recordCount + recordCountInCurrBatch;
        }else {
            hasFinished = true;
        }
        Util.sleepToSimulateDataHeavyProcessing();
        return  roadAccidentBatch;
    }

    public boolean hasFinished(){
        return hasFinished;
    }

	@Override
	public void run() {
	long start = System.currentTimeMillis();
		while(true){
			if(hasFinished()){				
				try {
					List<RoadAccident> exitList = new ArrayList<RoadAccident>();
					 RoadAccidentBuilder roadAccidentBuilder =  new RoadAccidentBuilder("StopEnrich");
					 RoadAccident ra = roadAccidentBuilder.build();
					exitList.add(ra);
					roadAccidentBlockingQueue.put(exitList);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				break;
			}
			try {
	        	roadAccidentBlockingQueue.put(getNextBatch());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}	
	}
}
