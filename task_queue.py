import sys
import os
import pika
import subprocess

def init_taskmq(queue_name):
	connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
	channel = connection.channel()
	channel.queue_declare(queue=queue_name)
	return connection, channel

def send_task(channel, message):
	channel.basic_publish(exchange='', routing_key='task_queue', body=message, properties=pika.BasicProperties(
	delivery_mode=2,)) # make the message persistent
	print(" [x] Sent %r"%message)

def close_connection(connection):
	connection.close()

def send_file(hosts_file, src_file, dest_path):
	p = subprocess.Popen("pscp -h %s -r -e send_err.log %s %s"%(hosts_file, src_file, dest_path), stdout=subprocess.PIPE, shell=True)
	(output, err) = p.communicate()
	print(output)
	print("......File transfer finished......")

def start_workers(hosts_file, dest_file):
	p = subprocess.Popen("pssh -h %s pkill -f %s"%(hosts_file, os.path.basename(dest_file)), shell=True)
	p.communicate()
	p = subprocess.Popen("pssh -i -h %s 'nohup ~/anaconda/bin/python %s > ~/out 2>&1 &'"%(hosts_file, dest_file), stdout=subprocess.PIPE, shell=True)
	(output, err) = p.communicate()
	print(output)
	print("......Workers start finished......")

def main():
	# copy file to remote workers
	send_file('hosts.txt', 'task_queue_worker.py', '~/')
	# start workers
	start_workers('hosts.txt', '~/task_queue_worker.py')
	
	message = ' '.join(sys.argv[1:]) or "Hello World!"	# define task
	connection, channel = init_taskmq('task_queue')

	send_task(channel, message)		# sent task

	close_connection(connection)

if __name__ == '__main__':
	main()
