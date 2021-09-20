
EXECBIN   = httpserver
COMPILECPP = gcc httpserver.c -o httpserver -pthread -Wall -Wextra -Wpedantic -Wshadow

all : spotless ${EXECBIN}

${EXECBIN} :
	${COMPILECPP}

clean :
	- rm *.o

spotless : clean
	- rm ${EXECBIN}
