# work-count

This is a different solution in Spark for a test traditional of Big Data, using data frame with select.

# Instructions

You will find two text files in this package: `us_constitution.txt` and `top_english_words.txt`. The former contains the contents of the United States Constitution, whereas the later contains the top 10,000 english words searched on google. Your goal is to find which words from the US Constitution are part of the top 10,000 words currently searched on google using Spark.

### Expected output

The expected output is supposed to be a **SINGLE FILE** under the `result` folder with the words from the United States Constitution that are part of the top 10,000 words searched on Google. The file must contain, on each line, the word followed by the number of times it appeared on the Constitution. This list must be sorted by frequency in descending order.

Even though the data provided would fit in any computer's RAM, 

Even though the data provided is small and would fit in any computer's RAM, you are supposed to write a **DISTRIBUTED ALGORITHM*, as if you were working on a large dataset.

Example (this is how the file should be formatted, the contents do not represent the expected result): 

```
the 10
of 9
and 7
to 6
or 4
in 3
by 2
...
```
