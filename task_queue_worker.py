import pika
import time

def callback(ch, method, properties, body):
	print(" [x] Received %r" % body)
	time.sleep(body.count(b'.'))	# executing task
	print(" [x] Done")
	ch.basic_ack(delivery_tag = method.delivery_tag)

def init_taskmq(host_ip, queue_name):
	connection = pika.BlockingConnection(pika.ConnectionParameters(host_ip))
	channel = connection.channel()
	channel.queue_declare(queue=queue_name)
	channel.basic_qos(prefetch_count=1)
	channel.basic_consume(callback, queue=queue_name)
	return connection, channel

def start_listen(channel):
	print(' [*] Waiting for message. To exit press CTRL+C')
	channel.start_consuming()

def main():
	_, channel = init_taskmq('localhost', 'task_queue')
	start_listen(channel)


if __name__ == '__main__':
	main()
