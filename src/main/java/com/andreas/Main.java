package com.andreas;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

/**
 * Hello world!
 */
public final class Main {
    // column file before eod
    private static int ID = 0;
    private static int BALANCED = 3;
    private static int PREV_BALANCED = 4;

    // column file after eod
    private static int AVG_BALANCE = 7;
    private static int FEE = 9;
    private static int THREAD1 = 8;
    private static int THREAD2A = 10;
    private static int THREAD2B = 4;
    private static int THREAD3 = 5;

    private static Character SEPARATOR = ';';

    // thread name
    private static String THREAD_1_NAME = "No 1 Thread-%d";
    private static String THREAD_2A_NAME = "No 2A Thread-%d";
    private static String THREAD_2B_NAME = "No 2B Thread-%d";
    private static String THREAD_3_NAME = "No 3 Thread-%d";

    private static int SIZE_THREAD_1 = 5;
    private static int SIZE_THREAD_2A = 4;
    private static int SIZE_THREAD_2B = 6;
    private static int SIZE_THREAD_3 = 8;

    private Main() {
    }

    private static List<String[]> loadData(String file) {
        try {
            FileReader filereader = new FileReader(file);
            CSVParser parser = new CSVParserBuilder().withSeparator(SEPARATOR).build();
            CSVReader csvReader = new CSVReaderBuilder(filereader).withCSVParser(parser).build();
            List<String[]> data = csvReader.readAll();
            csvReader.close();
            return data;
        } catch (FileNotFoundException e) {
            System.out.println("file " + file + " not found");
        } catch (IOException e) {
            System.out.println("error while read csv: " + e);
        }

        return null;
    }

    private static void writeFile (List<String[]> result) {
        String fileName = "After Eod.csv";
        try {
            FileWriter outputfile = new FileWriter(fileName);
            CSVWriter writer = new CSVWriter(outputfile);
            writer.writeAll(result);
            writer.close();
        } catch (Exception e) {
            System.out.println("error appear : " + e);
        }
  
    }

    private static BasicThreadFactory createThreadFactory (String name) {
        return new BasicThreadFactory.Builder()
            .namingPattern(name)
            .daemon(true)
            .priority(Thread.MAX_PRIORITY)
            .build();
    }

    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        long start = System.currentTimeMillis();
        String fileBefore = "Before Eod.csv";
        String fileAfter = "After Eod original.csv";
        List<String[]> dataBefore = loadData(fileBefore);
        List<String[]> dataAfter = loadData(fileAfter);

        // remove header on data before, because header is not used
        dataBefore.remove(0);
        
        try {

            ExecutorService executor1 = Executors.newFixedThreadPool(SIZE_THREAD_1, createThreadFactory(THREAD_1_NAME));
            ExecutorService executor2a = Executors.newFixedThreadPool(SIZE_THREAD_2A, createThreadFactory(THREAD_2A_NAME));
            ExecutorService executor2b = Executors.newFixedThreadPool(SIZE_THREAD_2B, createThreadFactory(THREAD_2B_NAME));
            ExecutorService executor3 = Executors.newFixedThreadPool(SIZE_THREAD_3, createThreadFactory(THREAD_3_NAME));

            for (String[] row: dataBefore) {
                executor1.submit(new Callable<String>() {  
                    public String call() throws Exception {  
                        Double balance = Double.valueOf(row[BALANCED]);
                        int id = Integer.valueOf(row[ID]);

                        Double avg = ( balance + Double.valueOf(row[PREV_BALANCED]) ) / 2.0;

                        // update avg balance
                        dataAfter.get(Integer.valueOf(id))[AVG_BALANCE] = avg.toString();
                        dataAfter.get(Integer.valueOf(id))[THREAD1] = Thread.currentThread().getName();
                        return "Task 1";  
                    }  
                });
            }
            executor1.shutdown();

            for (String[] row: dataBefore) {
                Double balance = Double.valueOf(row[BALANCED]);

                if (balance >= 100 && balance <= 150) {
                    // update fee transfer
                    executor2a.submit(new Callable<String>() {  
                        public String call() throws Exception {  
                            dataAfter.get(Integer.valueOf(row[ID]))[FEE] = "5";
                            dataAfter.get(Integer.valueOf(row[ID]))[THREAD2A] = Thread.currentThread().getName();
                            return "Task 2a";  
                        }  
                    });
                } else if (balance > 150) {
                    // update balance
                    executor2b.submit(new Callable<String>() {  
                        public String call() throws Exception {  
                            Double balance = Double.valueOf(row[BALANCED]) + 25;

                            dataAfter.get(Integer.valueOf(row[ID]))[BALANCED] = balance.toString();
                            dataAfter.get(Integer.valueOf(row[ID]))[THREAD2B] = Thread.currentThread().getName();

                            return "Task 2b";  
                        }  
                    });
                }                 
            }
            executor2a.shutdown();

            for (String[] row: dataAfter) {
                if (row[ID].equals("id")) continue;
                int id = Integer.valueOf(row[ID]);

                if (id <= 100) {
                    // update balance
                    executor3.submit(new Callable<String>() {  
                        public String call() throws Exception {  
                            Double balance = Double.valueOf(row[BALANCED]) + 10;
                            
                            dataAfter.get(Integer.valueOf(row[ID]))[BALANCED] = balance.toString();
                            dataAfter.get(Integer.valueOf(row[ID]))[THREAD3] = Thread.currentThread().getName();

                            // also update balance data before
                            dataBefore.get(Integer.valueOf(row[ID]))[BALANCED] = balance.toString();
                            return "Task 3";  
                        }  
                    });
                }
            }
            executor3.shutdown();

            writeFile(dataAfter);
        } catch (NumberFormatException e) {
            System.out.println("failed convert decimal " + e);
            e.printStackTrace();
        } catch (NullPointerException e) {
            System.out.println("access null pointer " + e);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("access empty index " + e);
        }
        System.out.println("finish after: " + (System.currentTimeMillis() - start) + " millis");
    }
}
