package com.andreas;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

/**
 * Hello world!
 */
public final class GenerateVerifyCSV {
    // column file before eod
    private static int ID = 0;
    private static int BALANCED = 3;
    private static int PREV_BALANCED = 4;

    // column file after eod
    private static int AVG_BALANCE = 7;
    private static int FEE = 9;

    private static Character SEPARATOR = ';';

    private GenerateVerifyCSV() {
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
        String fileName = "Verify Result.csv";
        try {
            FileWriter outputfile = new FileWriter(fileName);
            CSVWriter writer = new CSVWriter(outputfile);
            writer.writeAll(result);
            writer.close();
        } catch (Exception e) {
            System.out.println("error appear : " + e);
        }
  
    }

    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        String fileBefore = "Before Eod.csv";
        String fileAfter = "After Eod original.csv";
        List<String[]> dataBefore = loadData(fileBefore);
        List<String[]> dataAfter = loadData(fileAfter);

        // remove header on data before, because header is not used
        dataBefore.remove(0);
        
        try {
            for (String[] row: dataBefore) {
                Double balance = Double.valueOf(row[BALANCED]);
                int id = Integer.valueOf(row[ID]);
                System.out.println(id + " " + balance + " " + row[PREV_BALANCED]);
                Double avg = ( balance + Double.valueOf(row[PREV_BALANCED]) ) / 2.0;

                // update avg balance
                dataAfter.get(Integer.valueOf(row[ID]))[AVG_BALANCE] = avg.toString();

                if (balance >= 100 && balance <= 150) {
                    // update fee transfer
                    dataAfter.get(Integer.valueOf(row[ID]))[FEE] = "5";
                } else if (balance > 150) {
                    // update balance
                    balance += 25;
                    dataAfter.get(Integer.valueOf(row[ID]))[BALANCED] = balance.toString();
                }

                if (id <= 100) {
                    // update balance
                    balance += 10;
                    dataAfter.get(Integer.valueOf(row[ID]))[BALANCED] = balance.toString();
                }
            }

            writeFile(dataAfter);
        } catch (NumberFormatException e) {
            System.out.println("failed convert decimal " + e);
        } catch (NullPointerException e) {
            System.out.println("access null pointer " + e);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("access empty index " + e);
        }
        System.out.println("finish after: " + (System.currentTimeMillis() - start) + " millis");
    }
}
