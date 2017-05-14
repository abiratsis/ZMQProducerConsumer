using System;
using System.Collections.Concurrent;
using System.Threading;
using ZeroMQ;

namespace ZMQProducerConsumer.ParallelPipeline
{
    public class Sink
    {
        public static readonly ConcurrentBag<string> receivedList = new ConcurrentBag<string>();

        public static void Run(CancellationToken cancellationToken)
        {
            using (var context = new ZContext())
            using (var sink = new ZSocket(context, ZSocketType.PULL))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    Console.WriteLine("Sink cancelled...");

                    context.Shutdown();
                    cancellationToken.ThrowIfCancellationRequested();
                }

                Console.WriteLine("Sink started....");

                sink.Bind("tcp://*:5558");
                
                // Wait for start of batch
                ZError error;
                ZFrame frame = sink.ReceiveFrame(out error);

                if (null == frame)
                {
                    if (error != null)
                        throw new ZException(error);
                }

                while (!cancellationToken.IsCancellationRequested)
                {
                    using (frame = sink.ReceiveFrame(out error))
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

                        string item = frame.ReadString();
                        receivedList.Add(item);
                    }

                    if (cancellationToken.IsCancellationRequested)
                    {
                        Console.WriteLine("Sink cancelled...");
                        context.Shutdown();
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                }
            }
        }
    }
}
