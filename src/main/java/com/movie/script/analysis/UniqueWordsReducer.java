package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class UniqueWordsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Use a HashSet to collect unique words for the character
        HashSet<String> uniqueWordsSet = new HashSet<>();

        // Iterate over all words associated with the character
        for (Text val : values) {
            uniqueWordsSet.add(val.toString());
        }

        // Convert the set of unique words to a comma-separated string
        StringBuilder uniqueWords = new StringBuilder();
        for (String word : uniqueWordsSet) {
            if (uniqueWords.length() > 0) {
                uniqueWords.append(", ");
            }
            uniqueWords.append(word);
        }

        // Write the character and their unique words to the context
        context.write(key, new Text(uniqueWords.toString()));
    }
}