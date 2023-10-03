/*
 * the MapReduce functionality implemeted in this program takes a single large text file to map i.e. split it into small chunks and then assign 1 to all the found words
 * then reduces by adding count values to each unique words
 * To build: ./gradlew build
 * To run: ./gradlew run -PchooseMain=io.grpc.filesystem.task2.MapReduce --args="input/pigs.txt output/output-task2.txt"
 */

package io.grpc.filesystem.task2;

import java.util.stream.Collectors;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Map;
import java.util.Timer;

import io.grpc.filesystem.task2.Mapper;

public class MapReduce {

    public static String makeChunks(String inputFilePath) throws IOException {
        int count = 1;
        int size = 500;
        File f = new File(inputFilePath);
        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath))) {
            String l = br.readLine();

            while (l != null) {
                File newFile = new File(f.getParent() + "/temp", "chunk"
                        + String.format("%03d", count++) + ".txt");
                try (OutputStream out = new BufferedOutputStream(new FileOutputStream(newFile))) {
                    int fileSize = 0;
                    while (l != null) {
                        byte[] bytes = (l + System.lineSeparator()).getBytes(Charset.defaultCharset());
                        if (fileSize + bytes.length > size)
                            break;
                        out.write(bytes);
                        fileSize += bytes.length;
                        l = br.readLine();
                    }
                }
            }
        }
        return f.getParent() + "/temp";
    }

    /**
     * @param inputfilepath
     * @throws IOException
     */
    public static void map(String inputfilepath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(inputfilepath))) {
            String l = br.readLine();
            ArrayList<Mapper<String, Integer>> used = new ArrayList<>();
            while (l != null) {
                String[] words = l.toLowerCase().replaceAll("[\\p{Punct}]", "").split("\\s+");
                for (String word : words) {
                    if (!word.isEmpty()) {
                        boolean found = false;
                        for (int i = 0; i < used.size(); i++) {
                            if (used.get(i).getWord().equals(word)) {
                                used.set(i, new Mapper<>(word, used.get(i).getValue() + 1));
                                found = true;
                                break;
                            }
                        }
                        if (!found) used.add(new Mapper<>(word, 1));
                    }
                }
                l = br.readLine();
            }
            (new File("input/temp/map")).mkdir();
            try (OutputStream out = new BufferedOutputStream(new FileOutputStream(new File("input/temp/map/map-chunk"
                    + inputfilepath.substring(inputfilepath.length() - 7, inputfilepath.length() - 3) + "txt")))) {
                for (Mapper<String, Integer> object : used) {
                    out.write((object.getWord() + ":" + object.getValue() + "\n").getBytes(Charset.defaultCharset()));
                }
            }
        }
    }

    /**
     * @param inputfilepath
     * @param outputfilepath
     * @return
     * @throws IOException
     */
    public static void reduce(String inputfilepath, String outputfilepath) throws IOException {
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(outputfilepath)))) {
            try (BufferedReader br = new BufferedReader(new FileReader(outputfilepath))) {
                ArrayList<Mapper<String, Integer>> used = new ArrayList<>();
                File dir = new File(inputfilepath + "/map");
                File[] directoryListing = dir.listFiles();
                for (File f : directoryListing) {
                    try (BufferedReader br2 = new BufferedReader(new FileReader(f.getPath()))) {
                        String line = br2.readLine();
                        while (line != null) {
                            String[] pair2 = line.split(":");
                            boolean found = false;
                            for (int i = 0; i < used.size(); i++) {
                                if (used.get(i).getWord().equals(pair2[0])) {
                                    used.set(i, new Mapper<>(pair2[0], used.get(i).getValue() + Integer.parseInt(pair2[1])));
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) used.add(new Mapper<>(pair2[0], Integer.parseInt(pair2[1])));
                            line = br2.readLine();
                        }
                    }
                }
                for (int i = 0; i < used.size() - 1; i++) {
                    for (int j = 0; j < used.size() - i - 1; j++) {
                        if (used.get(j).getValue() < used.get(j+1).getValue()){
                            Mapper<String, Integer> temp = used.get(j);
                            used.set(j, used.get(j+1));
                            used.set(j + 1, temp);
                        }
                    }
                }
                for (Mapper<String, Integer> object : used) {
                    out.write((object.getWord() + ":" + object.getValue() + "\n").getBytes(Charset.defaultCharset()));
                }
            }
        }
    }

    /**
     * Takes a text file as an input and returns counts of each word in a text file
     * "output-task2.txt"
     * 
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException { // update the main function if required
        String inputFilePath = args[0];
        String outputFilePath = args[1];
        String chunkpath = makeChunks(inputFilePath);
        File dir = new File(chunkpath);
        File[] directoyListing = dir.listFiles();
        if (directoyListing != null) {
            for (File f : directoyListing) {
                if (f.isFile()) {

                    map(f.getPath());

                }
            }
            reduce(chunkpath, outputFilePath);

        }

    }
}