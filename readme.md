使用 init_main.py 来创建 dynamodb 的库，sqs 的消息队列。同时在 dynamodb 中插入一条初始数据。    
使用 deploy_lambda_main.py 来创建 lambda 所需的 policy，并上传 lambda 目录里的程序。    
使用 lambda/lambda.py 来监听dynamodb中的数据，将任务数据放入 sqs 中。  
使用 cli/worker_main.py 启动 worker，来消费 sqs 中的消息。

安装 supervisord 以后，可以直接用 supervisord 来启动程序  
但是需要修改 etc/supervisord.conf 里的 directory, 改成代码所在的绝对路径


[2017-07-20]

将 modules 从主项目分离：  
* 建立单独的 git 项目对modules进行管理
* 使用 submodule 将该项目关联到主项目

新增 cli/helper_main.py
* 监控指定 modules 项目的代码。  
  当该项目的 master 发生变化的时候，将代码下载到 gitcache 目录下，然后将发生变化的文件拷贝到 modules 目录里。

修改 cli/worker_main.py
* 使用 watchdog 监控 modules 目录，当文件更新时，通知程序重新加载模块代码
* 任务处理失败时 dynamodb 中的 jobstage 改为 failed 状态
* 增加 sqs 中可视时间的更新