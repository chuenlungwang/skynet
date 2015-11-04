#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/file.h>
#include <signal.h>
#include <errno.h>
#include <stdlib.h>

#include "skynet_daemon.h"

/* 检查 pidfile 文件中包含的进程号是否代表另外一个正在运行的进程.
 * 若是则返回此 pid, 若进程号代表的进程没有运行或者是当前进程号则返回 0 . */
static int
check_pid(const char *pidfile) {
	int pid = 0;
	FILE *f = fopen(pidfile,"r");
	/* 文件尚未创建, 自然不可能有另外运行的进程 */
	if (f == NULL)
		return 0;
	int n = fscanf(f,"%d", &pid);
	fclose(f);

	/* 文件中没有进程号内容, 进程号为 0 或者进程号与当前进程号相同,
	   表示没有另外一个正在运行的进程 */
	if (n !=1 || pid == 0 || pid == getpid()) {
		return 0;
	}

	/* 检查 pid 是否表示一个正在运行的进程 */
	if (kill(pid, 0) && errno == ESRCH)
		return 0;

	return pid;
}

/* 将当前进程的进程号写入到 pidfile 文件中. 若写入不成功返回 0, 如果写入成功则返回进程号.
 * 需要说明的是如果写入成功, pidfile 将一直以独占的方式打开直到进程结束. */
static int 
write_pid(const char *pidfile) {
	FILE *f;
	int pid = 0;

	/* 以读写模式和 644 权限打开文件, 如果文件不存在则创建文件 */
	int fd = open(pidfile, O_RDWR|O_CREAT, 0644);
	if (fd == -1) {
		fprintf(stderr, "Can't create %s.\n", pidfile);
		return 0;
	}
	/* 以读写方式打开 fd 文件描述符所表示的文件, 此函数属于 POSIX.1 中定义的函数,
	   返回的 FILE * 指针可以调用 fprintf 函数. */
	f = fdopen(fd, "r+");
	if (f == NULL) {
		fprintf(stderr, "Can't open %s.\n", pidfile);
		return 0;
	}

	/* 以非阻塞方式对 fd 所代表的文件加排它锁, 如果已经有别的进程对文件加锁, 表示
	   在 check_pid 到 write_pid 这段时间有别的 skynet 进程起来了. 这种情况将加锁失败.
	   加锁成功后将一直保持到进程结束. */
	if (flock(fd, LOCK_EX|LOCK_NB) == -1) {
		int n = fscanf(f, "%d", &pid);
		fclose(f);
		if (n != 1) {
			fprintf(stderr, "Can't lock and read pidfile.\n");
		} else {
			fprintf(stderr, "Can't lock pidfile, lock is held by pid %d.\n", pid);
		}
		return 0;
	}
	
	pid = getpid();
	/* [fix]当 fprintf 写入失败时将返回负数, 因而检查返回值小于等于 0 才对.[/fix] */
	if (!fprintf(f,"%d\n", pid)) {
		fprintf(stderr, "Can't write pid.\n");
		close(fd);
		return 0;
	}
	/* 刷新输出, 但不关闭文件, 因为需要保持文件锁. */
	fflush(f);

	return pid;
}

/* 将当前进程设置为守护进程, 参数 pidfile 表示将会写入进程号的文件路径. 此函数在 OS X 环境下无效.
 * 此函数只在进程启动时执行一次. 设置成功返回 0 , 失败返回 1 .
 * 如果 pidfile 文件中其它正在运行的进程号, 或者无法设置当前进程为守护进程,
 * 又或者无法将当前进程号写入 pidfile 时都将失败. */
int
daemon_init(const char *pidfile) {
	int pid = check_pid(pidfile);

	if (pid) {
		fprintf(stderr, "Skynet is already running, pid = %d.\n", pid);
		return 1;
	}

/* OS X 环境下不会设置守护进程 */
#ifdef __APPLE__
	fprintf(stderr, "'daemon' is deprecated: first deprecated in OS X 10.5 , use launchd instead.\n");
#else
	if (daemon(1,0)) {
		fprintf(stderr, "Can't daemonize.\n");
		return 1;
	}
#endif

	pid = write_pid(pidfile);
	if (pid == 0) {
		return 1;
	}

	return 0;
}

/* 执行守护进程清理动作, 这个函数会以安全方式删除 pidfile 文件. 此函数只在进程退出时执行一次. */
int 
daemon_exit(const char *pidfile) {
	return unlink(pidfile);
}
