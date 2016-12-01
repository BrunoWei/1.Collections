package com.epam.concurrency.task;

import com.epam.data.RoadAccident;
import com.epam.data.RoadAccidentBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tanmoy on 6/16/2016.
 */
public class AccidentDataEnricher implements Runnable {

	private PoliceForceExternalDataService policeForceService = new PoliceForceExternalDataService();

	protected BlockingQueue<List<RoadAccident>> roadAccidentBlockingQueue = null;

	protected BlockingQueue<List<RoadAccidentDetails>> roadAccidentDetailsBlockingQueue = null;
	
	private Logger log = LoggerFactory.getLogger(AccidentDataEnricher.class);

	public AccidentDataEnricher(BlockingQueue<List<RoadAccident>> roadAccidentBlockingQueue,
			BlockingQueue<List<RoadAccidentDetails>> roadAccidentDetailsBlockingQueue) {
		super();
		this.roadAccidentBlockingQueue = roadAccidentBlockingQueue;
		this.roadAccidentDetailsBlockingQueue = roadAccidentDetailsBlockingQueue;
	}

	public List<RoadAccidentDetails> enrichRoadAccidentData(List<RoadAccident> roadAccidents) {
		List<RoadAccidentDetails> roadAccidentDetailsList = new ArrayList<>(roadAccidents.size());
		for (RoadAccident roadAccident : roadAccidents) {
			roadAccidentDetailsList.add(enrichRoadAccidentDataItem(roadAccident));
		}
		Util.sleepToSimulateDataHeavyProcessing();
		return roadAccidentDetailsList;
	}

	public RoadAccidentDetails enrichRoadAccidentDataItem(RoadAccident roadAccident) {
		RoadAccidentDetails roadAccidentDetails = new RoadAccidentDetails(roadAccident);
		enrichPoliceForceContactSynchronously(roadAccidentDetails);
		/**
		 * above call might get blocked causing the application to get stuck
		 *
		 * solve this problem by accessing the the
		 * PoliceForceExternalDataService asynchronously with a timeout of 30 S
		 *
		 * use method "enrichPoliceForceContactAsynchronously" instead
		 */
		return roadAccidentDetails;
	}

	private void enrichPoliceForceContactSynchronously(RoadAccidentDetails roadAccidentDetails) {
		String policeForceContact = policeForceService.getContactNoWithoutDelay(roadAccidentDetails.getPoliceForce());
		roadAccidentDetails.setPoliceForceContact(policeForceContact);
	}

	private void enrichPoliceForceContactAsynchronously(RoadAccidentDetails roadAccidentDetails) {
		// use policeForceService.getContactNoWithDelay
	}

	@Override
	public void run() {		
		while (true) {			
			try {
				List<RoadAccidentDetails> roadAccidentDetailsList = new ArrayList<RoadAccidentDetails>();
				List<RoadAccident> roadAccidents = new ArrayList<RoadAccident>();
				roadAccidents = roadAccidentBlockingQueue.take();
				if (roadAccidents != null && roadAccidents.size() == 1 && roadAccidents.get(0).getAccidentId().equals("StopEnrich")) {
					List<RoadAccidentDetails> exitRoadAccidentDetailsList = new ArrayList<RoadAccidentDetails>();
					 RoadAccidentBuilder roadAccidentBuilder =  new RoadAccidentBuilder("StopWrite");
					 RoadAccident ra = roadAccidentBuilder.build();
					 exitRoadAccidentDetailsList.add(enrichRoadAccidentDataItem(ra));
					 roadAccidentDetailsBlockingQueue.put(exitRoadAccidentDetailsList);
					 break;				 					
				}else{
					for (RoadAccident roadAccident : roadAccidents) {
						roadAccidentDetailsList.add(enrichRoadAccidentDataItem(roadAccident));
					}
					roadAccidentDetailsBlockingQueue.put(roadAccidentDetailsList);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

	}
}
