import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MapReduce {
<<<<<<< HEAD
	
		static Map<String, String> input;
        
        public static void main(String[] args) {
               
                // SETUP:
        	
        			System.out.println("Running Test");
        			
        			// Get number of threads
        			
        			// From command line
        			// int threads = Integer.parseInt(args[0]);
        			
        			// Enter in programme
        			// int threads = 4;
        			int threads = Runtime.getRuntime().availableProcessors();
        			
        			Boolean printResults = false;
        			
        			System.out.println("Threads: " + threads + "\n");
        			
        			// Store filenames
        			ArrayList<String> filenames = new ArrayList<String>();
        			
//        			// Take inputs from command line
//        			int argIndex = 1;
//        			int recordFiles = 1;
//        			
//        			while(recordFiles == 1) {
//        				try {
//        					filenames.add(args[argIndex]);
//        					argIndex ++;
//        				}
//        				catch (ArrayIndexOutOfBoundsException e) {
//        					recordFiles = 0;
//        				}	
//        			}
        			
        			// Enter File Names here - quicker for debugging
        			filenames.add("shakespeare.txt");
        			filenames.add("big.txt");
        			filenames.add("austen.txt");
        			filenames.add("darwin.txt");
        			filenames.add("doyle.txt");
        			filenames.add("joyce.txt");
        			filenames.add("dickens.txt");
        			filenames.add("grimm.txt");
        			filenames.add("illiad.txt");
        			filenames.add("whitman.txt");
        			
        			// Original Filenames - Test for printing
//        			filenames.add("file1.txt");
//        			filenames.add("file2.txt");
//        			filenames.add("file3.txt");

        			
                // Input Map - read files and populate
                Map<String, String> input = new HashMap<String, String>();
                
                for(int i = 0; i < filenames.size(); i++) {
                	
	                	try {
		                	File file = new File(filenames.get(i));
			    			FileReader fileReader = new FileReader(file);
			    			BufferedReader bufferedReader = new BufferedReader(fileReader);
			    			StringBuffer stringBuffer = new StringBuffer();
			    			String line;
			    			while ((line = bufferedReader.readLine()) != null) {
			    				stringBuffer.append(line);
			    				stringBuffer.append("\n");
			    			}
			    			fileReader.close();
			    			String fileContents = stringBuffer.toString();
			    			input.put(filenames.get(i), fileContents);
	                } catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
                }
               
                // RUN TESTS:
                
                // Call brute force Method
                System.out.println("Running Brute Force Test");
                long startTime1 = System.currentTimeMillis();
                bruteForce(input,printResults);
                long finishTime1 = System.currentTimeMillis();
                long runTime1 = finishTime1 - startTime1;
                System.out.println("Run Time: " + runTime1 + "ms\n");
                
                // Call standard MapReduce Method
                System.out.println("Running Standard Map Reduce Test");
                long startTime2 = System.currentTimeMillis();
                mapReduce(input,printResults);
                long finishTime2 = System.currentTimeMillis();
                long runTime2 = finishTime2 - startTime2;
                System.out.println("Run Time: " + runTime2 + "ms\n");
                
                // Call distributed MapReduce Method - Original Implementation
                System.out.println("Running Distributed Map Reduce Test");
                long startTime3 = System.currentTimeMillis();
                mapReduceDist(input,printResults,threads);
                long finishTime3 = System.currentTimeMillis();
                long runTime3 = finishTime3 - startTime3;
                System.out.println("Run Time: " + runTime3 + "ms\n");
                
                // Call distributed MapReduce Method with Pool of Threads
                System.out.println("Running Distributed Map Reduce Test with Thread Pool");
                long startTime4 = System.currentTimeMillis();
                mapReduceDistPool(input,printResults,threads);
                long finishTime4 = System.currentTimeMillis();
                long runTime4 = finishTime4 - startTime4;
                System.out.println("Run Time: " + runTime4 + "ms\n");
                
                // Distributed MapReduce Method with parallel Group Phase with pool of threads
                System.out.println("Running Full Distributed Map Reduce Test with Thread Pool");
                long startTime5 = System.currentTimeMillis();
                mapReduceFullDistPool(input,printResults,threads);
                long finishTime5 = System.currentTimeMillis();
                long runTime5 = finishTime5 - startTime5;
                System.out.println("Run Time: " + runTime5 + "ms\n");
                
                System.out.println("Completed");
                
        }
        
        // APPROACH #1: Brute force
        public static void bruteForce(Map<String, String> input, boolean print){
                
        		Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
                
            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                    Map.Entry<String, String> entry = inputIter.next();
                    String file = entry.getKey();
                    String contents = entry.getValue();
                    
                    String[] words = contents.trim().split("\\s+");
                    
                    for(String word : words) {
                            
                            Map<String, Integer> files = output.get(word);
                            if (files == null) {
                                    files = new HashMap<String, Integer>();
                                    output.put(word, files);
                            }
                            
                            Integer occurrences = files.remove(file);
                            if (occurrences == null) {
                                    files.put(file, 1);
                            } else {
                                    files.put(file, occurrences.intValue() + 1);
                            }
                    }
            }
            
            if(print) {
            		// show me:
                System.out.println(output);
            }
        }

        // APPROACH #2: MapReduce
        public static void mapReduce(Map<String, String> input, boolean print){
        	
        		Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
                
            // MAP:
            
            List<MappedItem> mappedItems = new LinkedList<MappedItem>();
            
            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                    Map.Entry<String, String> entry = inputIter.next();
                    String file = entry.getKey();
                    String contents = entry.getValue();
                    
                    map(file, contents, mappedItems);
            }
            
            // GROUP:
            
            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();
            
            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                    MappedItem item = mappedIter.next();
                    String word = item.getWord();
                    String file = item.getFile();
                    List<String> list = groupedItems.get(word);
                    if (list == null) {
                            list = new LinkedList<String>();
                            groupedItems.put(word, list);
                    }
                    list.add(file);
            }
            
            // REDUCE:
            
            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while(groupedIter.hasNext()) {
                    Map.Entry<String, List<String>> entry = groupedIter.next();
                    String word = entry.getKey();
                    List<String> list = entry.getValue();
                    
                    reduce(word, list, output);
            }
            
            if(print) {
            		System.out.println(output);
            }
            
        }
        
        // APPROACH #3: Distributed MapReduce Original Implementation - Uncontrolled Thread Creation
        public static void mapReduceDist(Map<String, String> input, boolean print,int threads){
            final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
            
            // MAP:
            
            final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
            
            final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                    @Override
                    public synchronized void mapDone(String file, List<MappedItem> results) {
                    		mappedItems.addAll(results);
                    }
            };
            
            List<Thread> mapCluster = new ArrayList<Thread>(input.size());
            
            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                    Map.Entry<String, String> entry = inputIter.next();
                    final String file = entry.getKey();
                    final String contents = entry.getValue();
                    
                    Thread t = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                    map(file, contents, mapCallback);
                            }
                    });
                    
                    mapCluster.add(t);
                    t.start();
                   
            }
         
            // wait for mapping phase to be over:
            for(Thread t : mapCluster) {
                    try {
                            t.join();
                    } catch(InterruptedException e) {
                            throw new RuntimeException(e);
                    }
            }
           
            // GROUP:
            
            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();
            
            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                    MappedItem item = mappedIter.next();
                    String word = item.getWord();
                    String file = item.getFile();
                    List<String> list = groupedItems.get(word);
                    if (list == null) {
                            list = new LinkedList<String>();
                            groupedItems.put(word, list);
                    }
                    list.add(file);
            }
            
            // REDUCE:
            
            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                    @Override
                    public synchronized void reduceDone(String k, Map<String, Integer> v) {
                    	output.put(k, v);
                    }
            };
            
            List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());
            
            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while(groupedIter.hasNext()) {
                    Map.Entry<String, List<String>> entry = groupedIter.next();
                    final String word = entry.getKey();
                    final List<String> list = entry.getValue();
                    
                    Thread t = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                    reduce(word, list, reduceCallback);
                            }
                    });
                    
                    reduceCluster.add(t);
                    t.start();
                   
            }
            
            // wait for reducing phase to be over:
            for(Thread t : reduceCluster) {
                    try {
                            t.join();
                    } catch(InterruptedException e) {
                            throw new RuntimeException(e);
                    }
            }
            
            if(print) {
            		System.out.println(output);
            }
        }
        
        // Modified Method: APPROACH #4: Distributed MapReduce with Fixed Thread Pool
        public static void mapReduceDistPool(Map<String, String> input, boolean print,int threads){
            final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
            
            // MAP:
            
            final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
            
            final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                    @Override
                    public synchronized void mapDone(String file, List<MappedItem> results) {
                    		mappedItems.addAll(results);
                    }
            };
            
            // Use Executor to control number of running threads
            ExecutorService mapExecutor = Executors.newFixedThreadPool(threads);
            
            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                    Map.Entry<String, String> entry = inputIter.next();
                    final String file = entry.getKey();
                    final String contents = entry.getValue();
                    
                    Thread t = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                    map(file, contents, mapCallback);
                            }
                    });
                    
                    mapExecutor.execute(t);
            }
            
            mapExecutor.shutdown();
    		
    			while(!mapExecutor.isTerminated()) {
    				//Wait until terminated
    			}	
                        
            // GROUP:
            
            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();
            
            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                    MappedItem item = mappedIter.next();
                    String word = item.getWord();
                    String file = item.getFile();
                    List<String> list = groupedItems.get(word);
                    if (list == null) {
                            list = new LinkedList<String>();
                            groupedItems.put(word, list);
                    }
                    list.add(file);
            }
            
            // REDUCE:
            
            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                    @Override
                    public synchronized void reduceDone(String k, Map<String, Integer> v) {
                    	output.put(k, v);
                    }
            };
            
            ExecutorService reduceExecutor = Executors.newFixedThreadPool(threads);
            
            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while(groupedIter.hasNext()) {
                    Map.Entry<String, List<String>> entry = groupedIter.next();
                    final String word = entry.getKey();
                    final List<String> list = entry.getValue();
                    
                    Thread t = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                    reduce(word, list, reduceCallback);
                            }
                    });
                    
                    reduceExecutor.execute(t);
            }
            
            reduceExecutor.shutdown();
    		
			while(!reduceExecutor.isTerminated()) {
				//Wait until terminated
			}	
            
            if(print) {
            		System.out.println(output);
            }
        }
        
        
        // Modified Method: APPROACH #5: Fully Distributed MapReduce including Group Phase with Fixed Thread Pool
        public static void mapReduceFullDistPool(Map<String, String> input, boolean print,int threads){
            final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
            
            // MAP:
            
            final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
            
            final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                    @Override
                    public synchronized void mapDone(String file, List<MappedItem> results) {
                    		mappedItems.addAll(results);
                    }
            };
            
            ExecutorService mapExecutor = Executors.newFixedThreadPool(threads);
            
            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                    Map.Entry<String, String> entry = inputIter.next();
                    final String file = entry.getKey();
                    final String contents = entry.getValue();
                    
                    Thread t = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                    map(file, contents, mapCallback);
                            }
                    });
                    
                    mapExecutor.execute(t);
            }
            
            mapExecutor.shutdown();
    		
    			while(!mapExecutor.isTerminated()) {
    				//Wait until terminated
    			}	
                        
            // GROUP:
            
            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();
            
            // Implement parallelisation on this stage
            // Cannot have multiple key / word entries in the groupedItems map
            // Run Executor to batch process some of the results and then re-combine
            
            ExecutorService groupExecutor = Executors.newFixedThreadPool(threads);
            
            // Split into smaller groups based on the number of threads
            final ArrayList<LinkedList<MappedItem>> groupsList = new ArrayList<LinkedList<MappedItem>>();
            
            int entries = mappedItems.size();
            int splitSize = Math.round( (float)entries / (float)threads );
            
            int startIndex = 0;
            int finishIndex = (splitSize - 1);
            
            // Split mapped results according to the number of available threads
            for (int i = 0; i < threads; i++) {
            	
            		if (i == (threads - 1)) {
            			finishIndex = entries - 1;
            		}
            		
            		LinkedList<MappedItem> list = new LinkedList<MappedItem>(mappedItems.subList(startIndex, finishIndex+1));
            		
            		startIndex = finishIndex + 1;
            		finishIndex = finishIndex + splitSize;
            	
            		groupsList.add(list);
            }
            
            // Create Array List to store results from simultaneous group runs
            final ArrayList<HashMap<String,List<String>>> groupedResults = new ArrayList<HashMap<String,List<String>>>();
            
            // Start each group run dependning on how many threads are available
            for(int i = 0; i < threads; i++) {
            	
            		final int index = i;
            		
            		//Add empty entry to results list
            		groupedResults.add(new HashMap<String, List<String>>());
            	
	            	Thread t = new Thread(new Runnable() {
	                    @Override
	                    public void run() {
	                            group(groupsList.get(index), groupedResults, index);
	                    }
	            });
	            	
	            	 groupExecutor.execute(t);
            }
            
            groupExecutor.shutdown();
    		
			while(!groupExecutor.isTerminated()) {
				//Wait until terminated
			}	
            
            // Finalise Grouped Items - Combine results

			// Initialise with starting group
			groupedItems = groupedResults.get(0);
			
			// Iterate over remaining groups, perform a merge operation - Further investigate actual merge method?
			// If keys do not overlap, lists of entries are added
			// Else, entries are appended to existing key entries
			for (int i = 1 ; i < threads; i++) {
				
				 Iterator<Map.Entry<String, List<String>>> groupedIter = groupedResults.get(i).entrySet().iterator();
				 
				 while(groupedIter.hasNext()) {
					 
					 Map.Entry<String, List<String>> entry = groupedIter.next();
	                 final String word = entry.getKey();
	                 final List<String> list = entry.getValue();
	                 
	                 List<String> currentList = groupedItems.get(word);
	                 
	                 if(currentList == null) {
	                	 	currentList = new LinkedList<String>();
	                	 	groupedItems.put(word,list);
	                 }
	                 
					 currentList.addAll(list);	 
				 }
			}
            
            // REDUCE:
            
            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                    @Override
                    public synchronized void reduceDone(String k, Map<String, Integer> v) {
                    	output.put(k, v);
                    }
            };
            
            ExecutorService reduceExecutor = Executors.newFixedThreadPool(threads);
            
            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while(groupedIter.hasNext()) {
                    Map.Entry<String, List<String>> entry = groupedIter.next();
                    final String word = entry.getKey();
                    final List<String> list = entry.getValue();
                    
                    Thread t = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                    reduce(word, list, reduceCallback);
                            }
                    });
                    
                    reduceExecutor.execute(t);
            }
            
            reduceExecutor.shutdown();
    		
			while(!reduceExecutor.isTerminated()) {
				//Wait until terminated
			}	
            
            if(print) {
            		System.out.println(output);
            }
        }
        
        
        
        //Methods Used
        
        public static void map(String file, String contents, List<MappedItem> mappedItems) {
                String[] words = contents.trim().split("\\s+");
                for(String word: words) {
                        mappedItems.add(new MappedItem(word, file));
                }
        }
        
        public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
                Map<String, Integer> reducedList = new HashMap<String, Integer>();
                for(String file: list) {
                        Integer occurrences = reducedList.get(file);
                        if (occurrences == null) {
                                reducedList.put(file, 1);
                        } else {
                                reducedList.put(file, occurrences.intValue() + 1);
                        }
                }
                output.put(word, reducedList);
        }
        
        public static interface MapCallback<E, V> {
                
                public void mapDone(E key, List<V> values);
        }
        
        public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
                String[] words = contents.trim().split("\\s+");
                List<MappedItem> results = new ArrayList<MappedItem>(words.length);
                for(String word: words) {
                        results.add(new MappedItem(word, file));
                }
                callback.mapDone(file, results);
        }
        
        // Added Group Method
        // Implements similar functionality to above group procedure for passed parameters
        public static void group(LinkedList<MappedItem> inputList,  ArrayList<HashMap<String,List<String>>> results, int index) {
        	
        		Iterator<MappedItem> mappedIter = inputList.iterator();
            while(mappedIter.hasNext()) {
                    MappedItem item = mappedIter.next();
                    String word = item.getWord();
                    String file = item.getFile();
                    List<String> list = results.get(index).get(word);
                    if (list == null) {
                            list = new LinkedList<String>();
                            results.get(index).put(word, list);
                    }
                    list.add(file);
            }
        }
        
        public static interface ReduceCallback<E, K, V> {
                
                public void reduceDone(E e, Map<K,V> results);
        }
        
        public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {
                
                Map<String, Integer> reducedList = new HashMap<String, Integer>();
                for(String file: list) {
                        Integer occurrences = reducedList.get(file);
                        if (occurrences == null) {
                                reducedList.put(file, 1);
                        } else {
                                reducedList.put(file, occurrences.intValue() + 1);
                        }
                }
                callback.reduceDone(word, reducedList);
        }
        
        private static class MappedItem { 
                
                private final String word;
                private final String file;
                
                public MappedItem(String word, String file) {
                        this.word = word;
                        this.file = file;
                }

                public String getWord() {
                        return word;
                }

                public String getFile() {
                        return file;
                }
                
                @Override
                public String toString() {
                        return "[\"" + word + "\",\"" + file + "\"]";
                }
        }
} 
