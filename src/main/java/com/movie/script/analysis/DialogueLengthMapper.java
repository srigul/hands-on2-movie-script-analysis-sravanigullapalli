package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable wordCount = new IntWritable();
    private Text character = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line into character and dialogue (Assuming "Character: Dialogue" format)
        String line = value.toString();
        String[] parts = line.split(":", 2);
        
        if (parts.length == 2) {
            String characterName = parts[0].trim();
            String dialogue = parts[1].trim();

            // Tokenize the dialogue to count the number of words
            StringTokenizer tokenizer = new StringTokenizer(dialogue);
            int count = tokenizer.countTokens();

            // Set the character name and word count
            character.set(characterName);
            wordCount.set(count);

            // Emit the character and word count
            context.write(character, wordCount);
        }
    }
}