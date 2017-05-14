using System;
using System.Collections.Generic;
using System.Threading;

using ZeroMQ;

namespace ZMQProducerConsumer.ParallelPipeline
{
    public class Worker
    {
        private static readonly object _lock = new object();
        private readonly int workerId;

        public int ProcessedMessages;

        public static int WorkerNum;
        
        public Worker()
        {
            lock (_lock)
            {
                WorkerNum++;
                workerId = WorkerNum;
            }
        }

        public void Run(CancellationToken cancellationToken)
        {
            using (var context = new ZContext())
            using (var receiver = new ZSocket(context, ZSocketType.PULL))
            using (var sink = new ZSocket(context, ZSocketType.PUSH))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    Console.WriteLine($"Woker{workerId} cancelled....");

                    context.Shutdown();
                    cancellationToken.ThrowIfCancellationRequested();
                }

                receiver.Connect("tcp://127.0.0.1:5557");
                sink.Connect("tcp://127.0.0.1:5558");

                Console.WriteLine($"Worker{workerId} started....");

                while (!cancellationToken.IsCancellationRequested)
                {
                    int workload;
                    ZError error;

                    using (ZFrame frame = receiver.ReceiveFrame(out error))
                    {
                        if (frame == null)
                        {
                            if (Equals(error, ZError.EAGAIN))
                            {
                                Thread.Sleep(1);
                                continue;
                            }
                            throw new ZException(error);
                        }
                        workload = frame.ReadInt32();
                    }

                    sink.SendFrame(new ZFrame($"worker{workerId} workload:{workload}"));

                    ProcessedMessages++;

                    if (cancellationToken.IsCancellationRequested)
                    {
                        Console.WriteLine($"Woker{workerId} cancelled....");

                        context.Shutdown();
                        cancellationToken.ThrowIfCancellationRequested();
                    }

                    Thread.SpinWait(100);
                }
            }
        }
    }
}
