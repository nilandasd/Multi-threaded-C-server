#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <ctype.h>
#include <err.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

//#define DEBUG 1

//Mutexes and singly linked list used for handling new requests
pthread_mutex_t request_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t got_request = PTHREAD_COND_INITIALIZER;

struct request {
    int connfd;
    struct request* next;
};

struct request* requests = NULL;

/**
  mutexes and singly linked list used for ensuring exclusive access to files,
  including the log file
*/
pthread_mutex_t file_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t file_unlocked = PTHREAD_COND_INITIALIZER;

struct locked_file {
    char filename[17];
    struct locked_file* next;
};

struct locked_file* locked_files = NULL;

/**
  Global vars to signify if logging is being used and the log_filename
*/
char logfd = -1;
char log_filename[50];
pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;


//
pthread_mutex_t read_mutex = PTHREAD_MUTEX_INITIALIZER;

//#################### FILE LOCKER ############################################
//#############################################################################
/**
  Attempts to add the name of the locked file to the locked file list,
  if the file is already locked, the thread waits here until it recieves
  a broadcast that a file was unlocked.
*/
void lock_file(char* fname,
               pthread_mutex_t* p_mutex,
               pthread_cond_t*  p_cond_var) {
    struct locked_file* a_locked_file;
    struct locked_file* file_to_lock;
    file_to_lock = (struct locked_file*)malloc(sizeof(struct locked_file));

    pthread_mutex_lock(p_mutex);
    a_locked_file = locked_files;
    while(a_locked_file != NULL){
        if(strcmp(a_locked_file->filename, fname) == 0){
            a_locked_file = locked_files;
            pthread_cond_wait(p_cond_var, p_mutex);
            a_locked_file = locked_files;
        } else {
            a_locked_file = a_locked_file->next;
        }
    }
    file_to_lock->next = locked_files;
    strcpy(file_to_lock->filename, fname);
    locked_files = file_to_lock;
    pthread_mutex_unlock(p_mutex);
}


/**
  Takes the locked file out of the locked file list, if the file to be
  unlocked is not in the list, the program exits(this should never happen).
*/
void unlock_file(char* fname,
                 pthread_mutex_t* p_mutex,
                 pthread_cond_t*  p_cond_var) {
    struct locked_file* a_locked_file;
    struct locked_file* prev_locked_file;

    pthread_mutex_lock(p_mutex);
    a_locked_file = locked_files;
    prev_locked_file = locked_files;
    while(a_locked_file != NULL){
        if(strcmp(a_locked_file->filename, fname) == 0){
            prev_locked_file->next = a_locked_file->next;
            break;
        }
        prev_locked_file = a_locked_file;
        a_locked_file = a_locked_file->next;
    }
    if(a_locked_file == NULL) errx(EXIT_FAILURE, "unlock_file: file is not locked\n");
    if(a_locked_file == locked_files) locked_files = locked_files->next;
    free(a_locked_file);
    pthread_mutex_unlock(p_mutex);
    pthread_cond_broadcast(p_cond_var);
}

//#################### LOGGING ################################################
//#############################################################################
/**
  Used to log requests, and responses only if logging was turned on
*/
void log_success(char *request_type, char *filename, char *hostname, int content_len) {
    char log_entry[50];
    pthread_mutex_lock(&log_mutex);
    if(!logfd){
      pthread_mutex_unlock(&log_mutex);
      return;
    }
    sprintf(log_entry,
            "%s\t/%s\t%s\t%d\n",
             request_type, filename, hostname, content_len);
    write(logfd, log_entry, strlen(log_entry));
    pthread_mutex_unlock(&log_mutex);
}

void log_error(char *request, int code) {
    char log_entry[50];
    pthread_mutex_lock(&log_mutex);
    if(!logfd){
      pthread_mutex_unlock(&log_mutex);
      return;
    }
    sprintf(log_entry,
            "FAIL\t%s\t%d\n",
             request, code);
    write(logfd, log_entry, strlen(log_entry));
    pthread_mutex_unlock(&log_mutex);
}


//#################### CONNECTION HANDLING ####################################
//#############################################################################
/**
  Used by reply_get and reply_put to read bytes from the read fd and write to
  the write fd, via a 4mb buffer. Returns a 0 if all bytes were
  written, else -1.
*/
int readWriteBytes(int numBytes, int read_fd, int write_fd) {
    int status;
    int bytesRead = 0;
    char buffer[4096];
    while(1){
        status = read(read_fd, buffer, sizeof buffer);
        if (status < 0) return 1;
        bytesRead += status;
        status = write(write_fd, buffer, status);
        if (status < 0) return 1;
        if(bytesRead >= numBytes) break;
    }
    if (bytesRead != numBytes) {
        return 1;
    } else {
        return 0;
    }
}

/**
  This function reads a 4mb into a  newly opened file identified by the header
   until it has read all bytes sent or it has read bytes equal to the content
   length specified by the buffer. It then returns with 201. If the file cannot
   be overwritten, it returns 403. If the number of bytes written does not
   match up with the content length it returns 400 and closes the clients
   connection. If it has an error writing the file it returns a 500.
*/
int reply_put(int connfd, int content_len, char *filename, char *hostname, char *request) {
    int status = 1;
    int filefd;
    char request_type[] = "PUT";
    if((access(filename, F_OK) == 0) && (access(filename, W_OK) != 0)){
        char forbidden[] = "HTTP/1.1 403 Forbidden\r\nContent-Length: 10\r\n\r\nForbidden\n";
        send(connfd, forbidden, strlen(forbidden), MSG_NOSIGNAL);
        log_error(request, 403);
        return 0;
    }
    filefd = open(filename, O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, 0644);
    status = readWriteBytes(content_len, connfd, filefd);
    close(filefd);
    if(status) {
        char bad_request[] = "HTTP/1.1 400 Bad Request\r\nContent-Length: 12\r\n\r\nBad Request\n";
        send(connfd, bad_request, strlen(bad_request), MSG_NOSIGNAL);
        log_error(request, 400);
        return 1;
    } else {
        char created[] = "HTTP/1.1 201 Created\r\nContent-Length: 8\r\n\r\nCreated\n";
        send(connfd, created, strlen(created), MSG_NOSIGNAL);
        log_success(request_type, filename, hostname, content_len);
        return 0;
    }
}

/**
    This function first gets the file size of a file via stat() and then opens
    that file specified by the header, and then sends a 200 response followed by
    the file in 4 mb chunks at a time which are put into a buffer before being
    written to the socket. If the requested file does not exist, the server returns
    404. If the requested file does not have read permissions the server
    responded 403. If the requested file is a directory, the server sends 400.
    If the server has an error reading the file it does not return 500 as the
    client would likely just write the 500 response to the file, so instead the
    function only returns.
*/
int reply_get(int connfd, char *filename, char *hostname, char *request) {
    char request_type[] = "GET";
    if(access(filename, F_OK) != 0){
        char not_found[] = "HTTP/1.1 404 Not Found\r\nContent-Length: 10\r\n\r\nNot Found\n";
        log_error(request, 404);
        send(connfd, not_found, strlen(not_found), MSG_NOSIGNAL);
        return 0;
    }else if(access(filename, R_OK) != 0){
        char forbidden[] = "HTTP/1.1 403 Forbidden\r\nContent-Length: 10\r\n\r\nForbidden\n";
        log_error(request, 403);
        send(connfd, forbidden, strlen(forbidden), MSG_NOSIGNAL);
        return 0;
    }
    int filefd = open(filename, O_RDONLY | O_SYNC);
    struct stat st;
    stat(filename, &st);
    if(S_ISDIR(st.st_mode)){
        char bad_request[] = "HTTP/1.1 400 Bad Request\r\nContent-Length: 12\r\n\r\nBad Request\n";
        log_error(request, 400);
        send(connfd, bad_request, strlen(bad_request), MSG_NOSIGNAL);
        return 1;
    }
    char get_response[50];
    sprintf(get_response,
            "HTTP/1.1 200 OK\r\nContent-Length: %ld\r\n\r\n",
            st.st_size);
    log_success(request_type, filename, hostname, st.st_size);
    send(connfd, get_response, strlen(get_response), MSG_NOSIGNAL);
    readWriteBytes(st.st_size, filefd, connfd);
    close(filefd);
    return 0;
}

/**
  This function first gets the file size of a file via stat() and then opens that
  file specified by the header, and then sends a 200 response followed by the file
  in 4 mb chunks at a time which are put into a buffer before being written to the
  socket. If the requested file does not exist, the server returns 404. If the
  requested file does not have read permissions the server responded 403. If the
  requested file is a directory, the server sends 400. If the server has an error
  reading the file it does not return 500 as the client would likely just write
  the 500 response to the file, so instead the function only returns.
*/
int reply_head(int connfd, char *filename, char *hostname, char *request) {
    char request_type[] = "HEAD";
    if(access(filename, F_OK) != 0){
        char not_found[] = "HTTP/1.1 404 Not Found\r\nContent-Length: 10\r\n\r\nNot Found\n";
        log_error(request, 404);
        send(connfd, not_found, strlen(not_found), MSG_NOSIGNAL);
        return 0;
    }else if(access(filename, R_OK) != 0){
        char forbidden[] = "HTTP/1.1 403 Forbidden\r\nContent-Length: 10\r\n\r\nForbidden\n";
        send(connfd, forbidden, strlen(forbidden), MSG_NOSIGNAL);
        return 0;
    }
    struct stat st;
    stat(filename, &st);
    if(S_ISDIR(st.st_mode)){
        char bad_request[] = "HTTP/1.1 400 Bad Request\r\nContent-Length: 12\r\n\r\nBad Request\n";
        log_error(request, 400);
        send(connfd, bad_request, strlen(bad_request), MSG_NOSIGNAL);
        return 1;
    }
    char head_response[50];
    sprintf(head_response,
            "HTTP/1.1 200 OK\r\nContent-Length: %ld\r\n\r\n",
             st.st_size);
    log_success(request_type, filename, hostname, st.st_size);
    send(connfd, head_response, strlen(head_response), MSG_NOSIGNAL);
    return 0;
}

/**
  Given in the request header, after field "Content-Length: "
*/
int get_content_length(char *buffer) {
    char content_len[] = "Content-Length: ";
    char *ptr = strstr(buffer, content_len);
    if(ptr != NULL){
        return atoi(ptr+16);
    } else {
        return -1;
    }
}

/**
  Given in the request header, after field "Host: "
*/
int get_hostname(char *buffer, char *hostname) {
    char host_field[] = "Host: ";
    char *host = strstr(buffer, host_field);
    if(host == NULL) return 1;
    int n = sscanf(host, "%*s %s", hostname);
    if(n != 1){
        return 1;
    } else {
        return 0;
    }
}

/**
  filename must be 15 chars long, and be alpha numeric
*/
int invalidFilename(char *filename) {
   if(strlen(filename) != 15) return 1;
   for(int i = 0; i <= 14; i++){
       if(!isalnum(filename[i])) return 1;
   }
   return 0;
}

/**
  Where a worker thread is sent after recieving a request with a connfd. Here it
  recieves a request from the accepted socket and returns a response.
*/
void handle_connection(int connfd){
    char buffer[4096];
    char request[50];
    char firstline[50];
    char filename[18];
    char version[50];
    char hostname[50];
    int bytes_read = 0;
    int status;
    int content_len;
    char *ptr;
    char bad_request[] = "HTTP/1.1 400 Bad Request\r\nContent-Length: 12\r\n\r\nBad Request\n";
    char server_error[] = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 22\r\n\r\nInternal Server Error\n";
    char not_implemented[] = "HTTP/1.1 501 Not Implemented\r\nContent-Length: 16\r\n\r\nNot Implemented\n";


    memset(buffer, 0, 4096);
    //MAINTAIN CONNECTION, EXIT ONLY ON BAD REQUEST
    while(1){
        //READING HEADER
        status = recv(connfd, buffer + bytes_read, 1, 0);
        if(status == -1) {
            send(connfd, server_error, strlen(server_error), MSG_NOSIGNAL);
            close(connfd);
            return;
        }
        if (status == 0) {
            close(connfd);
            return;
        }
        bytes_read += status;
        if(bytes_read > 4096){
            send(connfd, bad_request, strlen(bad_request), MSG_NOSIGNAL);
            close(connfd);
            return;
        }
        ptr = strstr(buffer, "\r\n\r\n");
        if (ptr == NULL) continue;


        //SUCCESSFUL READ OF HEADER
        bytes_read = 0;
        status = sscanf(buffer,"%s %*c%s %s", request, filename, version);
        filename[17] = '\0';
        content_len = get_content_length(buffer);
        ptr = strstr(buffer, "\r\n");
        memcpy(firstline, buffer, ptr - buffer);
        if(get_hostname(buffer, hostname)){
            send(connfd, bad_request, strlen(bad_request), MSG_NOSIGNAL);
            log_error(firstline, 400);
            close(connfd);
            return;
        }
        memset(buffer, 0, 4096);

        //EXIT IF HEADER IS INVALID
        if((status != 3) || (invalidFilename(filename)) ||
           (strcmp(version,"HTTP/1.1") != 0)){
            send(connfd, bad_request, strlen(bad_request), MSG_NOSIGNAL);
            log_error(firstline, 400);
            close(connfd);
            return;
        }

        //RESPOND TO HEADER
        if(!strcmp("PUT", request)){
            lock_file(filename, &file_mutex, &file_unlocked);
            if(content_len == -1){
                send(connfd, bad_request, strlen(bad_request), MSG_NOSIGNAL);
                log_error(firstline, 400);
                close(connfd);
                unlock_file(filename, &file_mutex, &file_unlocked);
                return;
            }
            if(reply_put(connfd, content_len, filename, hostname, firstline)) {
              close(connfd);
              unlock_file(filename, &file_mutex, &file_unlocked);
              return;
            }
            unlock_file(filename, &file_mutex, &file_unlocked);
        } else if(!strcmp("GET", request)){
            lock_file(filename, &file_mutex, &file_unlocked);
            if(reply_get(connfd, filename, hostname, firstline)) {
                close(connfd);
                unlock_file(filename, &file_mutex, &file_unlocked);
                return;
            }
            unlock_file(filename, &file_mutex, &file_unlocked);

        } else if(!strcmp("HEAD", request)){
            lock_file(filename, &file_mutex, &file_unlocked);
            if(reply_head(connfd, filename, hostname, firstline)) {
                close(connfd);
                unlock_file(filename, &file_mutex, &file_unlocked);
                return;
            }
            unlock_file(filename, &file_mutex, &file_unlocked);
        } else if((!strcmp("POST"   ,request)) ||
                  (!strcmp("DELETE" ,request)) ||
                  (!strcmp("TRACE"  ,request)) ||
                  (!strcmp("OPTIONS",request)) ||
                  (!strcmp("CONNECT",request)) ||
                  (!strcmp("PATCH"  ,request))){
            send(connfd, not_implemented, strlen(not_implemented), MSG_NOSIGNAL);
            log_error(firstline, 501);
        }else{
            send(connfd, bad_request, strlen(bad_request), MSG_NOSIGNAL);
            log_error(firstline, 400);
            close(connfd);
            return;
        }
    }
}



//#################### THREAD DISPATCHING #####################################
//#############################################################################
/**
  Used by the dispatcher thread, to add requests to a singly linked list,
  which will be consumed by the worker threads. Sends a signal whenever, a
  request is added, so that a waiting thread can be handle it.
*/
void add_request(int connfd,
                 pthread_mutex_t* p_mutex,
                 pthread_cond_t*  p_cond_var) {
    struct request* a_request = (struct request*)malloc(sizeof(struct request));

    if (!a_request) {
        errx(EXIT_FAILURE, "add_request: out of memory\n");
    }
        a_request->connfd = connfd;
    pthread_mutex_lock(p_mutex);
    a_request->next = requests;
    requests = a_request;
    pthread_mutex_unlock(p_mutex);
    pthread_cond_signal(p_cond_var);
}
/**
  Used by worker threads to get the connfd of an accepted socketed, so that the
  thread can handle the connection of that socket, and unlock the mutex. Else,
  the thread holds the lock until it reaches the wait in the request loop.
*/
int get_request(pthread_mutex_t* p_mutex){
    struct request* a_request;
    if (requests == NULL) return -1;
	  a_request = requests;
	  requests = a_request->next;
    int connfd = a_request->connfd;
    free(a_request);
    pthread_mutex_unlock(p_mutex);
    return connfd;
}
/**
  Worker threads will wait on pthread_cond_wait while there is no request to
  handle. Once they get a request they will handle the connection, until the
  client sends a bad request, and then the thread will wait for another request.
*/
void* handle_requests_loop(void* data){
    int connfd;
    pthread_mutex_lock(&request_mutex);
    while (1) {
        connfd = get_request(&request_mutex);
        if (connfd != -1) {
            handle_connection(connfd);
            pthread_mutex_lock(&request_mutex);
        } else {
            pthread_cond_wait(&got_request, &request_mutex);
        }
    }
}

//#################### MAIN DISPATCHER THREAD #################################
//#############################################################################
/**
   Converts a string to an 16 bits unsigned integer.
   Returns 0 if the string is malformed or out of the range.
 */
uint16_t strtouint16(char number[]) {
    char *last;
    long num = strtol(number, &last, 10);
    if (num <= 0 || num > UINT16_MAX || *last != '\0') {
        return 0;
    }
    return num;
}
/**
   Creates a socket for listening for connections.
   Closes the program and prints an error message on error.
 */
int create_listen_socket(uint16_t port) {
    struct sockaddr_in addr;
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0) {
        err(EXIT_FAILURE, "socket error");
    }

    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htons(INADDR_ANY);
    addr.sin_port = htons(port);
    if (bind(listenfd, (struct sockaddr*)&addr, sizeof addr) < 0) {
        err(EXIT_FAILURE, "bind error");
    }

    if (listen(listenfd, 500) < 0) {
        err(EXIT_FAILURE, "listen error");
    }

    return listenfd;
}

/**
  Gets the parses the cmd line args, to get the port, num threads flag, and
  logging flag. Exits the program if any flags or arguments are invalid, and
  prints a explanation.
*/
void parseArgs(int argc, char *argv[],
               uint16_t *port, int *num_threads) {
    if (argc < 2 || argc > 6) {
        errx(EXIT_FAILURE, "Wrong arguments: %s [port_num, -L (log file name), -N (# threads)\n", argv[0]);
    }
    for(int i = 1; i < argc; i++){
        if(strcmp(argv[i], "-N") == 0){
             if((i+1) == argc){
                 errx(EXIT_FAILURE, "Must Provide Flag -N argument: -N (# threads)\n");
             }
             *num_threads = strtouint16(argv[i + 1]);
             if (*num_threads <= 0) {
                 errx(EXIT_FAILURE, "Invalid number of threads: %s\n", argv[i+1]);
             }
             i++;
        } else if(strcmp(argv[i], "-l") == 0){
             if((i+1) == argc){
                 errx(EXIT_FAILURE, "Must Provide Flag -L argument: -L (log file name)\n");
             }
             strcpy(log_filename, argv[i + 1]);
             logfd = open(log_filename, O_CREAT | O_WRONLY | O_TRUNC | O_SYNC, 0644);
             i++;
        } else {
             *port = strtouint16(argv[i]);
             if (*port == 0) {
                 errx(EXIT_FAILURE, "invalid port number: %s\n", argv[i]);
             }
        }
    }
    if (*port == 1) {
        errx(EXIT_FAILURE, "Wrong arguments: %s [port_num, -L (log file name), -N (# threads)\n", argv[0]);
    }
}

/**
  The main thread creates a listen thread, and whenever it creates a accepted socket
  it addes the connected socket to a list of requests which are read by the worker
  threads. Never exits.
*/
int main(int argc, char *argv[]) {
    int listenfd;
    uint16_t port = 1;
    int connfd;
    int num_threads = 4;
    //parse the cmd line args, and exit if invalid
    parseArgs(argc, argv, &port, &num_threads);
    //make threads
    pthread_t p_threads[num_threads];
    int thr_id[num_threads];
    for (int i=0; i<num_threads; i++) {
	      thr_id[i] = i;
	      pthread_create(&p_threads[i], NULL, handle_requests_loop, (void*)&thr_id[i]);
    }
    //enter listening loop and create threads as connections are made
    listenfd = create_listen_socket(port);
    while(1) {
        connfd = accept(listenfd, NULL, NULL);
        if (connfd < 0) {
            warn("accept error");
            continue;
        }
        add_request(connfd, &request_mutex, &got_request);
    }
    /*  UNREACHABLE  */
    return EXIT_SUCCESS;
}
