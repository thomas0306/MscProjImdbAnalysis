 To run data cleaning:
 
 hadoop jar imdb.jar cleaning.DataCleaningDriver <input file name> output_datacleaning/

 <input file name> could be: in1.txt, in2.txt or in3.txt

 To run top-k clique analysis:

hadoop jar imdb.jar chaining.cliquen.FindCliqueN <n>

<n> could be: 2, 3 or 4