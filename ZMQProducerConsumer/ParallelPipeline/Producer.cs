using System;
using System.Threading;
using ZeroMQ;

namespace ZMQProducerConsumer.ParallelPipeline
{
    public class Producer
    {
        public static void Run(int messages, CancellationToken cancellationToken)
        {
            using (var context = new ZContext())
            using (var sender = new ZSocket(context, ZSocketType.PUSH))
            using (var sink = new ZSocket(context, ZSocketType.PUSH))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    Console.WriteLine("Producer cancelled...");

                    context.Shutdown();
                    cancellationToken.ThrowIfCancellationRequested();
                }

                Console.WriteLine($"Producer started....");

                sender.Bind("tcp://*:5557");
                sink.Connect("tcp://127.0.0.1:5558");

                // The first message is "0" and signals start of batch
                sink.Send(new byte[] { 0x00 }, 0, 1);

                int i = 1;
                for (; i <= messages; ++i)
                {
                    byte[] action = BitConverter.GetBytes(i);

                    sender.Send(action, 0, action.Length);

                    if (cancellationToken.IsCancellationRequested)
                    {
                        Console.WriteLine("Producer cancelled...");

                        context.Shutdown();
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                }
            }
        }
    }
}
