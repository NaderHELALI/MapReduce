using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using System;
using System.IO;


namespace MapReduce
{
    public class Program
    {
        public static ConcurrentBag<string> wordBag = new ConcurrentBag<string>();
        public static BlockingCollection<string> wordChunks = new BlockingCollection<string>(wordBag);

        public static IEnumerable<string> produceWordBlocks(string fileText)
        {
            int blockSize = 250;
            int startPos = 0;
            int len = 0;

            for (int i = 0; i < fileText.Length; i++)
            {
                i = i + blockSize > fileText.Length - 1 ? fileText.Length - 1 : i + blockSize;

                while (i >= startPos && fileText[i] != ' ')
                {
                    i--;
                }

                if (i == startPos)
                {
                    i = i + blockSize > (fileText.Length - 1) ? fileText.Length - 1 : i + blockSize;
                    len = (i - startPos) + 1;
                }
                else
                {
                    len = i - startPos;
                }

                yield return fileText.Substring(startPos, len).Trim();
                startPos = i;
            }
        }


        public static void mapWords(string fileText)
        {
            
            Parallel.ForEach(produceWordBlocks(fileText), wordBlock =>
            {   //split the block into words
                string[] words = wordBlock.Split(' ','\n');
                StringBuilder wordBuffer = new StringBuilder();

                //cleanup each word and map it
                foreach (string word in words)
                {   //Remove all spaces and punctuation
                    foreach (char c in word)
                    {
                        if (char.IsLetterOrDigit(c) || c == '\'' || c == '-')
                            wordBuffer.Append(c);
                    }
                    //Send word to the wordChunks Blocking Collection
                    if (wordBuffer.Length > 0)
                    {
                        wordChunks.Add(wordBuffer.ToString());
                        wordBuffer.Clear();
                    }
                }
            });

            wordChunks.CompleteAdding();
        }

        public static ConcurrentDictionary<string, int> wordStore = new ConcurrentDictionary<string, int>();

        public static void reduceWords()
        {
            Parallel.ForEach(wordChunks.GetConsumingEnumerable(), word =>
            {   //if the word exists, use a thread safe delegate to increment the value by 1
                //otherwise, add the word with a default value of 1
                wordStore.AddOrUpdate(word, 1, (key, oldValue) => Interlocked.Increment(ref oldValue));
            });
        }

        public static void mapReduce(string fileText)
        {   //Reset the Blocking Collection, if already used
            if (wordChunks.IsAddingCompleted)
            {
                wordBag = new ConcurrentBag<string>();
                wordChunks = new BlockingCollection<string>(wordBag);
            }

            //Create background process to map input data to words
            System.Threading.ThreadPool.QueueUserWorkItem(delegate (object state)
            {
                mapWords(fileText);
            });

            //Reduce mapped words
            reduceWords();
            foreach (var temp in wordStore) {

                Console.WriteLine(temp.Key + " : " + temp.Value);
            }
            
        }



    
    static void Main(string[] args)
        {
            string chemin = @"C:\Users\Asus\Documents\Cours Esilv\A4\S1\Design Pattern\Ex2\MapReduce\mapReduce.txt";
            string file = File.ReadAllText(chemin);

            Console.WriteLine("------- File ----------");
            Console.WriteLine(file);
            Console.WriteLine("\n------- Map Reduce ----------");
            
            mapReduce(file);
        }
    
    }
}

