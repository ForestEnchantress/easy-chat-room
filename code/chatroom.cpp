#include"head.h"
#define USER_LIMIT 5
#define BUFFER_SIZE 1024
#define FD_LIMIT 65535
#define MAX_EVENT_NUMBER 1024
#define PROCESS_LIMIT 65536
struct client_data{
	struct sockaddr_in address;
	int connfd;
	pid_t pid;
	int pipefd[2];
};
static const char *shm_name="/my_shm";
int sig_pipefd[2];
int epollfd;
int listenfd;
int shmfd;
char *share_mem=0;
client_data *users=0;
int *sub_process=0;
int user_count=0;
bool stop_child=false;

int setnonblocking(int fd){
	int old_option=fcntl(fd,F_GETFL);
	fcntl(fd,F_SETFL,old_option|O_NONBLOCK);
	return old_option;
}
void addfd(int epollfd,int fd){
	struct epoll_event event;
	event.data.fd=fd;
	event.events=EPOLLIN|EPOLLET;
	epoll_ctl(epollfd,EPOLL_CTL_ADD,fd,&event);
	setnonblocking(fd);
}
void sig_handler(int sig){
	int save_errno=errno;
	int msg=sig;
	send(sig_pipefd[1],(char*)&msg,1,0);
	errno=save_errno;
}
void addsig(int sig,void (*handler)(int),bool restart=true){
	struct sigaction sa;
	memset(&sa,'\0',sizeof(sa));
	sa.sa_handler=handler;
	if(restart){
		sa.sa_flags|=SA_RESTART;
	}
	sigfillset(&sa.sa_mask);
	assert(sigaction(sig,&sa,NULL)!=-1);
}
void del_resource(){
	close(sig_pipefd[0]);
	close(sig_pipefd[1]);
	close(listenfd);
	close(epollfd);
	shm_unlink(shm_name);
	delete[] users;
	delete[] sub_process;
}
void child_term_handler(int sig){
	stop_child=true;
}
int run_child(int idx,client_data *users,char *share_mem){
	struct epoll_event events[MAX_EVENT_NUMBER];

	int child_epollfd=epoll_create(5);
	assert(child_epollfd!=-1);
	int connfd=users[idx].connfd;
	addfd(child_epollfd,connfd);
	int pipefd=users[idx].pipefd[1];
	addfd(child_epollfd,pipefd);
	addsig(SIGTERM,child_term_handler,false);

	while(!stop_child){
		int ret=epoll_wait(child_epollfd,events,MAX_EVENT_NUMBER,-1);
		if((ret<0)&&(errno!=EINTR)){
			printf("errno:%d\n",errno);
			break;
		}
		for(int i=0;i<ret;++i){
			int sockfd=events[i].data.fd;
			if((sockfd==connfd)&&(events[i].events&EPOLLIN)){
				memset(share_mem+idx*BUFFER_SIZE,'\0',sizeof(BUFFER_SIZE));
				int num=recv(connfd,share_mem+idx*BUFFER_SIZE,BUFFER_SIZE-1,0);
				if(num<0){
					if(errno!=EAGAIN){
						stop_child=true;
					}
				}
				else if(num==0){
					stop_child=true;
				}
				else{
					send(pipefd,(char*)&idx,sizeof(idx),0);
				}
			}
			else if((sockfd==pipefd)&&(events[i].events&EPOLLIN)){
				int client=0;
				int num=recv(pipefd,(char*)&client,sizeof(client),0);
				if(num<0){
					if(errno!=EAGAIN){
						stop_child=true;
					}
				}
				else if(num==0){
					stop_child=0;
				}
				else{
					send(connfd,share_mem+client*BUFFER_SIZE,BUFFER_SIZE,0);
				}
			}
			else{
				continue;
			}
		}
	}
	close(child_epollfd);
	close(pipefd);
	close(connfd);
	return 0;
}

int main(int argc,char** argv){
	if(argc<=2){
		printf("Usage:%s ip port\n",argv[0]);
		exit(1);
	}
	const char *ip=argv[1];
	int port=atoi(argv[2]);

	struct sockaddr_in serv_adr;
	bzero(&serv_adr,sizeof(serv_adr));
	serv_adr.sin_family=AF_INET;
	inet_pton(AF_INET,ip,&serv_adr.sin_addr);
	serv_adr.sin_port=htons(port);

	int listenfd=socket(PF_INET,SOCK_STREAM,0);
	assert(listenfd>0);
	int ret=bind(listenfd,(SA*)&serv_adr,sizeof(serv_adr));
	assert(ret!=-1);

	ret=listen(listenfd,5);
	assert(ret!=-1);

	user_count=0;
	users=new client_data[USER_LIMIT+1];
	sub_process=new int[PROCESS_LIMIT];
	for(int i=0;i<PROCESS_LIMIT;++i){
		sub_process[i]=-1;
	}

	struct epoll_event events[MAX_EVENT_NUMBER];
	epollfd=epoll_create(5);
	assert(epollfd!=-1);
	addfd(epollfd,listenfd);
	ret=socketpair(PF_UNIX,SOCK_STREAM,0,sig_pipefd);
	assert(ret!=-1);
	setnonblocking(sig_pipefd[1]);
	addfd(epollfd,sig_pipefd[0]);

	shmfd=shm_open(shm_name,O_CREAT|O_RDWR,0666);
	assert(shmfd!=-1);
	ret=ftruncate(shmfd,USER_LIMIT*BUFFER_SIZE);
	assert(ret!=-1);
	share_mem=(char*)mmap(NULL,USER_LIMIT*BUFFER_SIZE,PROT_READ|PROT_WRITE,MAP_SHARED,shmfd,0);
	assert(share_mem!=MAP_FAILED);
	close(shmfd);

	addsig(SIGTERM,sig_handler);
	addsig(SIGINT,sig_handler);
	addsig(SIGCHLD,sig_handler);
	addsig(SIGPIPE,SIG_IGN);

	bool stop_server=false;
	bool terminate=false;
	while(!stop_server){
		int num=epoll_wait(epollfd,events,MAX_EVENT_NUMBER,-1);
		if((num<0)&&(errno!=EINTR)){
			printf("%d\n",errno);
			
			break;
		}
		for(int i=0;i<num;++i){
			int sockfd=events[i].data.fd;
			if(sockfd==listenfd){
				struct sockaddr_in clnt_adr;
				socklen_t clientlen=sizeof(clnt_adr);
				int connfd=accept(listenfd,(SA*)&clnt_adr,&clientlen);
				if(connfd<0){
					printf("errno is %d\n",errno);
					continue;
				}
				if(user_count>=USER_LIMIT){
					const char *info="too many users\n";
					printf("%s",info);
					send(connfd,info,strlen(info),0);
					close(connfd);
					continue;
				}
				users[user_count].address=clnt_adr;
				users[user_count].connfd=connfd;
				ret=socketpair(PF_UNIX,SOCK_STREAM,0,users[user_count].pipefd);
				assert(ret!=-1);

				pid_t pid=fork();
				if(pid<0){
					close(connfd);
					continue;
				}
				else if(pid==0){
					close(epollfd);
					close(listenfd);
					close(sig_pipefd[0]);
					close(sig_pipefd[1]);
					close(users[user_count].pipefd[0]);
					run_child(user_count,users,share_mem);
					munmap((void*)share_mem,user_count*BUFFER_SIZE);
					exit(0);
				}
				else{
					close(connfd);
					close(users[user_count].pipefd[1]);
					addfd(epollfd,users[user_count].pipefd[0]);
					users[user_count].pid=pid;
					sub_process[pid]=user_count;
					user_count++;
				}
			}
			else if((sockfd==sig_pipefd[0])&&(events[i].events&EPOLLIN)){
				char signals[1024];
				memset(signals,'\0',sizeof(signals));
				ret=recv(sig_pipefd[0],signals,1023,0);
				if(ret==-1){
					continue;
				}
				else if(ret==0){
					continue;
				}
				else{
					for(int i=0;i<ret;++i){
						switch(signals[i]){
							case SIGCHLD:
								pid_t pid;
								while((pid=waitpid(-1,NULL,WNOHANG))>0){
									int idx=sub_process[pid];
									sub_process[pid]=-1;
									if((idx<0)||(idx>USER_LIMIT)){
										continue;
									}
									epoll_ctl(epollfd,EPOLL_CTL_DEL,
										users[idx].pipefd[0],0);
									close(users[idx].pipefd[0]);
									users[idx]=users[--user_count];
									sub_process[users[idx].pid]=idx;
								}
								if((user_count==0)&&terminate){
									stop_server=true;
								}
								break;
							case SIGTERM:
							case SIGINT:
								printf("kill all the child now\n");
								if(user_count==0){
									stop_server=true;
									break;
								}
								for(int i=0;i<user_count;++i){
									pid_t pid=users[i].pid;
									kill(pid,SIGTERM);
								}
								terminate=true;
								break;
							default:
								break;
						}
					}
				}
			}
			else if(events[i].events&EPOLLIN){
				int child=0;
				ret=recv(sockfd,(char*)&child,sizeof(child),0);
				printf("read data from child across pipe\n");
				if(ret<0){
					continue;
				}
				else if(ret==0){
					continue;
				}
				else{
					for(int j=0;j<user_count;++j){
						if(users[j].pipefd[0]!=sockfd){
							printf("send data to child across pipe\n");
							send(users[j].pipefd[0],(char*)&child,sizeof(child),0);
						}
					}
				}
			}
		}
	}
	del_resource();
	return 0;
}

