package org.example;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONObject;

//class that used to load the correct query results
public class ReferenceLoader {
    public static Set<String> loadReferenceData(String filePath) throws Exception {
        Set<String> referenceSet = new HashSet<>();
        Files.lines(Paths.get(filePath)).forEach(line -> {
            if (line != null && !line.trim().isEmpty() && !line.trim().equals("null")) {
                referenceSet.add(line.trim());
            }
        });
        return referenceSet;
    }
}

