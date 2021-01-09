using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace MapReduce_without_thread
{

    class Program
    {
        //Import the data // Input
        static StreamReader ImportFile(string path)
        {
            StreamReader text = new StreamReader(path);
            return text;
        }

        //Splitting and Mapping the data
        static List<Tuple<string, int>> Mapping(StreamReader file)
        {
            List<Tuple<string, int>> mapping = new List<Tuple<string, int>>();

            string ln;
            while ((ln = file.ReadLine()) != null)
            {
                foreach (string word in ln.Split(" "))
                {
                    Tuple<string, int> temp = new Tuple<string, int>(word, 1);
                    mapping.Add(temp);
                }
            }
            return mapping;
        }
        //Print the Mapping
        static void PrintMapping(List<Tuple<string, int>> list)
        {
            Console.WriteLine("\n------- Step 2 : MAPPING ----------");
            foreach (Tuple<string, int> temp in list)
            {

                Console.WriteLine(temp.Item1 + ", " + temp.Item2);
            }
        }

        static Dictionary<string, List<int>> Shuffling(List<Tuple<string, int>> mapping)
        {
            Dictionary<string, List<int>> shuffling = new Dictionary<string, List<int>>();

            foreach (Tuple<string, int> temp in mapping)
            {

                if (shuffling.ContainsKey(temp.Item1) == true)
                {
                    shuffling[temp.Item1].Add(temp.Item2);

                }
                else
                {
                    List<int> value = new List<int>();
                    value.Add(1);
                    shuffling.Add(temp.Item1, value);
                }
            }
            var l = shuffling.OrderBy(key => key.Key);
            var dic = l.ToDictionary((keyItem) => keyItem.Key, (valueItem) => valueItem.Value);
            return dic;
        }
        static void PrintShuffling(Dictionary<string, List<int>> shuffling)
        {
            Console.WriteLine("\n------- Step 3 : SHUFFLING ----------");
            foreach (string temp in shuffling.Keys)
            {

                Console.WriteLine(temp + ", " + printList(shuffling[temp]));
            }
        }
        static string printList(List<int> value)
        {
            string res = "( ";
            foreach (int temp in value)
            {
                res += temp.ToString() + " ";
            }
            return res + ")";
        }


        static Dictionary<string, int> Reducing(Dictionary<string, List<int>> shuffling)
        {
            Dictionary<string, int> reduce = new Dictionary<string, int>();
            foreach (var temp in shuffling)
            {
                reduce.Add(temp.Key, temp.Value.Count);
            }
            return reduce;
        }
        static void PrintReduce(Dictionary<string, int> reduce)
        {
            Console.WriteLine("\n------- Step 4: Reduce ----------");
            foreach (var temp in reduce)
            {
                Console.WriteLine(temp.Key + ", " + temp.Value);
            }
        }
        static void Main(string[] args)
        {
            Console.WriteLine("\n------- Step 1 : INPUT ----------");
            StreamReader file = ImportFile(@"C:\Users\Asus\Documents\Cours Esilv\A4\S1\Design Pattern\Ex2\MapReduce\mapReduce.txt");
            Console.WriteLine("Data has been imported");
            List<Tuple<string, int>> mapping = Mapping(file);
            PrintMapping(mapping);

            Dictionary<string, List<int>> shuffling = Shuffling(mapping);
            PrintShuffling(shuffling);

            Dictionary<string, int> reduce = Reducing(shuffling);
            PrintReduce(reduce);
        }
    }
}
