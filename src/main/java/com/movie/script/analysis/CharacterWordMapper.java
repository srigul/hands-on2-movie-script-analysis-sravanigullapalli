package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text characterWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(":", 2);  // Assuming "Character: Dialogue" format
        if (parts.length == 2) {
            String character = parts[0].trim();
            String dialogue = parts[1].trim();
            
            StringTokenizer tokenizer = new StringTokenizer(dialogue);
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
                if (!word.isEmpty()) {
                    characterWord.set(character + "_" + word);
                    context.write(characterWord, one);
                }
            }
        }
    }
}