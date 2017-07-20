使用 init_main.py 来创建 dynamodb 的库，sqs 的消息队列。同时在 dynamodb 中插入一条初始数据。    
使用 deploy_lambda_main.py 来创建 lambda 所需的 policy，并上传 lambda 目录里的程序。  
使用 lambda/lambda.py 来监听dynamodb中的数据，将任务数据放入 sqs 中。
使用 cli/worker_main.py 启动 worker，来消费 sqs 中的消息。

安装 supervisord 以后，可以直接用 supervisord 来启动程序
但是需要修改 etc/supervisord.conf 里的 directory, 改成代码所在的绝对路径
