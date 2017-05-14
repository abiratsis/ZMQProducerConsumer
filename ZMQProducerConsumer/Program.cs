using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ZMQProducerConsumer.ParallelPipeline;

namespace ZMQProducerConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            CancellationTokenSource tokenSource = new CancellationTokenSource();
            CancellationToken ct = tokenSource.Token;

            Console.Write("Insert num of workers:");
            int worker_num = int.Parse(Console.ReadLine());

            Console.Write("Insert producer messages:");
            int messages = int.Parse(Console.ReadLine());

            Console.Write("Insert maximun execution time(sec):");
            int exectime = int.Parse(Console.ReadLine());

            Console.WriteLine("Press ESC to interupt!");
            Console.WriteLine();

            var tasks = new List<Task>();

            Task sink = Task.Factory.StartNew(() => Sink.Run(ct), ct);
            tasks.Add(sink);

            List<Worker> workers = new List<Worker>(worker_num);
            for (int i = 0; i < worker_num; i++)
            {
                Worker worker= new Worker();
                tasks.Add(Task.Factory.StartNew(() => worker.Run(ct), ct));
                workers.Add(worker);
            }

            while (Worker.WorkerNum < worker_num)
            {
                Thread.Sleep(10);
            }

            Task producer = Task.Factory.StartNew(() => Producer.Run(messages, ct), ct);
            tasks.Add(producer);

            Task watcher = Task.Factory.StartNew(() => WatchCancel(tokenSource));
            tasks.Add(watcher);

            Stopwatch timer = new Stopwatch();
            timer.Start();

            try
            {
                Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(exectime));
            }
            catch (AggregateException)
            {
                Console.WriteLine("\nAggregateException thrown with the following inner exceptions:");
            }
            
            timer.Stop();

            var processed = workers.Select(w => w.ProcessedMessages).Sum();

            PrintReport(messages, processed, timer.Elapsed.Seconds);

            Console.WriteLine("\nPress any key to exit.");
            Console.ReadKey(true);

        }

        public static void PrintReport(int total, int processedMessages, int duration)
        {
            int rate = processedMessages / duration;
            Console.WriteLine($"\nProcessed {processedMessages}/{total} in {duration} sec. Processing rate [{rate} m/sec]");
            //Sink.receivedList.ToList().ForEach(Console.WriteLine);
        }

        private static void WatchCancel(CancellationTokenSource cts)
        {
            while (true)
            {
                if (Console.ReadKey().Key == System.ConsoleKey.Escape)
                {
                    cts.Cancel();
                    break;
                }
                Thread.SpinWait(100);
            }
        }
    }
}
