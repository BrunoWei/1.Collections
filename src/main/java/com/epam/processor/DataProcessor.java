package com.epam.processor;

import com.epam.data.MyComparator;
import com.epam.data.RoadAccident;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This is to be completed by mentees
 */
public class DataProcessor {

    private final List<RoadAccident> roadAccidentList;

    public DataProcessor(List<RoadAccident> roadAccidentList){
        this.roadAccidentList = roadAccidentList;
    }


//    First try to solve task using java 7 style for processing collections

    /**
     * Return road accident with matching index
     * @param index
     * @return
     */
    public RoadAccident getAccidentByIndex7(String index){
    	for(RoadAccident ra : this.roadAccidentList){
    		if(index.equals(ra.getAccidentId())){
    			return ra;
    		}
    	}
        return null;
    }


    /**
     * filter list by longtitude and latitude values, including boundaries
     * @param minLongitude
     * @param maxLongitude
     * @param minLatitude
     * @param maxLatitude
     * @return
     */
    public Collection<RoadAccident> getAccidentsByLocation7(float minLongitude, float maxLongitude, float minLatitude, float maxLatitude){
    	Collection<RoadAccident> roadAccidentList = new ArrayList<RoadAccident>();
    	for(RoadAccident ra : this.roadAccidentList){
    		if(minLongitude <= ra.getLongitude() && ra.getLongitude() <= maxLongitude && minLatitude <= ra.getLatitude() && ra.getLatitude() <= maxLatitude){
    			roadAccidentList.add(ra);
    		}
    	}
        return roadAccidentList;
    }

    /**
     * count incidents by road surface conditions
     * ex:
     * wet -> 2
     * dry -> 5
     * @return
     */
    public Map<String, Long> getCountByRoadSurfaceCondition7(){
    	Map<String, Long> rtnMap = new HashMap<String, Long>();
    	for(RoadAccident ra : this.roadAccidentList){
    		String key = ra.getRoadSurfaceConditions();
    		Long value = rtnMap.get(key);
    		if(rtnMap.containsKey(key)){
    			rtnMap.put(key, ++value);
    		}else{
    			rtnMap.put(key, 1L);
    		}
    	}
    	
        return rtnMap;
    }

    /**
     * find the weather conditions which caused the top 3 number of incidents
     * as example if there were 10 accidence in rain, 5 in snow, 6 in sunny and 1 in foggy, then your result list should contain {rain, sunny, snow} - top three in decreasing order
     * @return
     */
    public List<String> getTopThreeWeatherCondition7(){
    	Map<String, Long> rtnMap = new HashMap<String, Long>();   	
    	for(RoadAccident ra : this.roadAccidentList){
    		String key = ra.getWeatherConditions();
    		Long value = rtnMap.get(key);
    		if(rtnMap.containsKey(key)){
    			rtnMap.put(key, ++value);
    		}else{
    			rtnMap.put(key, 1L);
    		}
    	}
    	MyComparator comparator = new MyComparator(rtnMap);
    	SortedMap<String, Long> sortedMap = new TreeMap<String, Long>(comparator); 
    	sortedMap.putAll(rtnMap);
    	List<String> top3List = new ArrayList<String>();
    	for(int i=2; i>=0; --i){
    		top3List.add(sortedMap.keySet().toArray()[i].toString());
    	}
        return top3List;
    }

    /**
     * return a multimap where key is a district authority and values are accident ids
     * ex:
     * authority1 -> id1, id2, id3
     * authority2 -> id4, id5
     * @return
     */
    public Multimap<String, String> getAccidentIdsGroupedByAuthority7(){
    	ListMultimap<String, String> multiMap = ArrayListMultimap.create();  	
    	for(RoadAccident ra : this.roadAccidentList){ 
    		multiMap.put(ra.getDistrictAuthority(),ra.getAccidentId());
    	}
        return multiMap;
    }


    // Now let's do same tasks but now with streaming api



    public RoadAccident getAccidentByIndex(String index){
    	RoadAccident roadAccident = this.roadAccidentList.stream().filter(item -> index.equals(item.getAccidentId()))
    			                                                  .findAny()
    	                                                          .orElse(null);   	           
        return roadAccident;
    }


    /**
     * filter list by longtitude and latitude fields
     * @param minLongitude
     * @param maxLongitude
     * @param minLatitude
     * @param maxLatitude
     * @return
     */
    public Collection<RoadAccident> getAccidentsByLocation(float minLongitude, float maxLongitude, float minLatitude, float maxLatitude){
    	Collection<RoadAccident> rtnList = this.roadAccidentList.stream().filter(item -> item.getLongitude() >= minLongitude)
    	                              .filter(item -> item.getLongitude() <= maxLongitude)
    	                              .filter(item -> item.getLatitude() >= minLatitude)
    	                              .filter(item -> item.getLatitude() <= maxLatitude)
    	                              .collect(Collectors.toList());
        return rtnList;
    }

    /**
     * find the weather conditions which caused max number of incidents
     * @return
     */
    public List<String> getTopThreeWeatherCondition(){
    	List<String> to3List = this.roadAccidentList.stream().collect(Collectors.groupingBy(RoadAccident::getWeatherConditions, Collectors.counting()))
    			                                                 .entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
    			                                                 .limit(3)
    			                                                 .map(item  -> item.getKey())
    			                                                 .collect(Collectors.toList());

         return to3List;
    }

    /**
     * count incidents by road surface conditions
     * @return
     */
    public Map<String, Long> getCountByRoadSurfaceCondition(){
    	Map<String, Long> rtnMap = this.roadAccidentList.stream().collect(Collectors.groupingBy(RoadAccident::getRoadSurfaceConditions, Collectors.counting()));
        return rtnMap;
    }

    /**
     * To match streaming operations result, return type is a java collection instead of multimap
     * @return
     */
    public Map<String, List<String>> getAccidentIdsGroupedByAuthority(){
    	Map<String, List<String>> rtnMap = this.roadAccidentList.stream()
    			                                                .collect(Collectors.groupingBy(RoadAccident::getDistrictAuthority,Collectors.mapping(RoadAccident::getAccidentId, Collectors.toList())));

        return rtnMap;
    }

}
