package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class UniqueWordsMapper extends Mapper<Object, Text, Text, Text> {

    private Text character = new Text();
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line into character and dialogue (assuming "Character: Dialogue" format)
        String line = value.toString();
        String[] parts = line.split(":", 2);
        
        if (parts.length == 2) {
            String characterName = parts[0].trim();
            String dialogue = parts[1].trim();

            // Use a HashSet to track unique words in this dialogue line
            HashSet<String> uniqueWords = new HashSet<>();
            StringTokenizer tokenizer = new StringTokenizer(dialogue);

            while (tokenizer.hasMoreTokens()) {
                // Clean and normalize the word
                String token = tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
                if (!token.isEmpty()) {
                    uniqueWords.add(token);
                }
            }

            // Emit each unique word associated with the character
            character.set(characterName);
            for (String uniqueWord : uniqueWords) {
                word.set(uniqueWord);
                context.write(character, word);
            }
        }
    }
}
