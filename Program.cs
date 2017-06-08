using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

class SurveyWorker
{
    private  static readonly string  _hostname = "localhost";
    private  static readonly string _username = "abc";
    private static readonly string _password = "123";

    public static void Main(string[] args)
    {
        PublishToScheduleQueue(GetMessage(args));

        var tasks = new []
        {
            Task.Factory.StartNew(()=> ConsumeWorkQueue()),
            Task.Factory.StartNew(()=> ConsumeProcessQueue())
        };

        Task.WaitAll(tasks);
    }
    
    private static void ConsumeWorkQueue()
    {
        var factory = new ConnectionFactory(){HostName = "localhost", UserName="abc" , Password="123"};
        
        using(var connection = factory.CreateConnection())
        using(var channel = connection.CreateModel())
        {
            channel.ExchangeDeclare("WORK_EXCHANGE", "direct");

            channel.QueueDeclare(queue:"work_task_queue", 
            durable:true, 
            exclusive:false, 
            autoDelete:false, 
            arguments: null);

            channel.QueueBind("work_task_queue","WORK_EXCHANGE", String.Empty, null);


            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

            var consumer = new EventingBasicConsumer(channel);

            consumer.Received += (model, ea) => 
            {
                var body = ea.Body;
                var message = Encoding.UTF8.GetString(body);
                
                Console.WriteLine("Got Message From WORK_EXCHANGE  :"+ message);

                // Based on the message process and result, we can send ACK or NACK. 
                // To ensure the message is not lost.
                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                //channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple:false, requeue:true);
            };

            channel.BasicConsume(queue:"work_task_queue", autoAck: false, consumer:consumer);

            Console.WriteLine("WORK_EXCHANGE ---- Press [Enter] to exit");
            Console.ReadLine();
        }  
    }

    private static void ConsumeProcessQueue()
    {
        var factory = new ConnectionFactory(){HostName = _hostname, UserName = _username , Password = _password};
        
        using(var connection = factory.CreateConnection())
        using(var channel = connection.CreateModel())
        {
            channel.ExchangeDeclare("PROCESS_EXCHANGE", "direct");

            channel.QueueDeclare(queue:"process_task_queue", 
            durable:true, 
            exclusive:false, 
            autoDelete:false, 
            arguments: null);

            channel.QueueBind("process_task_queue","PROCESS_EXCHANGE", String.Empty, null);


            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

            var consumer = new EventingBasicConsumer(channel);

            consumer.Received += (model, ea) => 
            {
                var body = ea.Body;
                var message = Encoding.UTF8.GetString(body);
                
                Console.WriteLine("Got Message PROCESS_EXCHANGE :"+ message);

                var result = MessageToRequeue(message);

                if (result)
                {
                   if (PublishToScheduleQueue(message))
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                   else
                        channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple:false, requeue:true);

                }
                else
                {
                    if (PublishToWorkQueue(message))
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    else
                        channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple:false, requeue:true);
                }
            };

            channel.BasicConsume(queue:"process_task_queue", autoAck: false, consumer:consumer);

            Console.WriteLine("\nPROCESS_EXCHANGE ---- Press [Enter] to exit");
            Console.ReadLine();
        }
    }

    private static bool PublishToWorkQueue(string message)
    {
        try
        {
            var factory = new ConnectionFactory(){HostName = _hostname, UserName = _username , Password = _password};
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare("WORK_EXCHANGE", "direct");

                channel.QueueDeclare(queue: "work_task_queue",
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);

                channel.QueueBind("work_task_queue", "WORK_EXCHANGE", string.Empty, null);

                var properties = channel.CreateBasicProperties();
                properties.Persistent = true;

                var body = Encoding.UTF8.GetBytes(message);

                Console.WriteLine("\nWORK_EXCHANGE [x] Publishing {0}", message);

                channel.BasicPublish(exchange: "WORK_EXCHANGE",
                routingKey: string.Empty,
                basicProperties: properties,
                body: body);

                return true;
            }
        }
        catch(Exception)
        {
            return false;
        }
    }
    
    private static bool PublishToScheduleQueue(string message)
    {
        try
        {
            var factory = new ConnectionFactory(){HostName = _hostname, UserName = _username , Password = _password};
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                IDictionary<String, Object> arguments = new Dictionary<String, Object>();
                arguments.Add("x-dead-letter-exchange", "PROCESS_EXCHANGE");

                channel.ExchangeDeclare("SCHEDULE_EXCHANGE", "direct");

                channel.QueueDeclare(queue: "schedule_task_queue",
                                        durable: true,
                                        exclusive: false,
                                        autoDelete: false,
                                        arguments: arguments);

                channel.QueueBind("schedule_task_queue", "SCHEDULE_EXCHANGE", string.Empty, null);

                var properties = channel.CreateBasicProperties();
                properties.Persistent = true;
                properties.Expiration = "15000";

                var body = Encoding.UTF8.GetBytes(message);
                    
                channel.BasicPublish(exchange: "SCHEDULE_EXCHANGE",
                                        routingKey: string.Empty,
                                        basicProperties: properties,
                                        body: body);

                Console.WriteLine("\nSCHEDULE_EXCHANGE [x] Sent {0}", message);

                return true;
            }
        }
        catch(Exception)
        {
            return false;
        }
    }

    private static bool MessageToRequeue(string message)
    {
        if (message.Equals("expires"))
        {
            return false;
        }
        else
        {
            return false;
        }
    }

    private static string GetMessage(string[] args)
    {
        return ((args.Length > 0) ? string.Join(" ", args) : "Hello World! Dead");
    }
}