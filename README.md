# easy-chat-room
1.统计事件源，信号事件和IO事件都由epoll监听

2.共享内存用以维护对所有客户端的读缓存区

3.高效的半同步/半异步模式，连接socket由工作进程管理（也用epoll监听）。工作进程监听到读事件，读数据到缓存区，并通过管道通知主进程广播。

4.主进程负责监听：信号、监听socket、与各子进程的管道fd。
