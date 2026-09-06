/* A deterministic PTY workload, not an Agent simulator or a capacity policy. */
#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <termios.h>
#include <sys/ioctl.h>
#include <time.h>
#include <unistd.h>

static char interrupt_line[128];
static size_t interrupt_len;

static void interrupted(int sig) {
    (void)sig;
    ssize_t ignored = write(STDOUT_FILENO, interrupt_line, interrupt_len);
    (void)ignored;
}

static int emit(const void *data, size_t length) {
    const unsigned char *p = data;
    while (length) {
        ssize_t n = write(STDOUT_FILENO, p, length);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        p += n;
        length -= (size_t)n;
    }
    return 0;
}

int main(int argc, char **argv) {
    if (argc >= 3 && !strcmp(argv[1], "--exec-tty")) {
        /* Parent creates a new session; acquire its owned PTY, then replace
           this helper with the real CLI without a Python pre-exec callback. */
        if (ioctl(STDIN_FILENO, TIOCSCTTY, 0)) return 8;
        execv(argv[2], argv + 2);
        return 9;
    }
    /* The signal handler cannot allocate: reserve "INT ", newline and NUL
       inside its fixed buffer. This protects only fixture labels. */
    if ((argc != 2 && argc != 3) || strlen(argv[1]) + sizeof("INT \n") > sizeof interrupt_line) return 2;
    const char *label = argv[1];
    struct termios attrs;
    if (tcgetattr(STDIN_FILENO, &attrs)) return 3;
    attrs.c_lflag &= (tcflag_t)~(ICANON | ECHO | ECHONL | IEXTEN);
    attrs.c_lflag |= ISIG;
    attrs.c_iflag &= (tcflag_t)~(ICRNL | INLCR | IGNCR | IXON);
    attrs.c_oflag &= (tcflag_t)~OPOST;
    attrs.c_cc[VMIN] = 1;
    attrs.c_cc[VTIME] = 0;
    attrs.c_cc[VINTR] = 3;
    if (tcsetattr(STDIN_FILENO, TCSANOW, &attrs)) return 4;
    interrupt_len = (size_t)snprintf(interrupt_line, sizeof interrupt_line, "INT %s\n", label);
    struct sigaction action = {0};
    action.sa_handler = interrupted;
    action.sa_flags = SA_RESTART;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGINT, &action, NULL)) return 5;
    char line[256];
    int n = snprintf(line, sizeof line, "READY %s %ld\n", label, (long)getpid());
    if (emit(line, (size_t)n)) return 6;
    size_t used = 0;
    for (;;) {
        unsigned char c;
        ssize_t got = read(STDIN_FILENO, &c, 1);
        if (got < 0 && errno == EINTR) continue;
        if (got <= 0) return 0;
        if (c != '\n') {
            if (used + 1 < sizeof line) line[used++] = (char)c;
            continue;
        }
        line[used] = 0;
        used = 0;
        unsigned long count, seed;
        if (!strncmp(line, "A ", 2)) {
            if (argc != 3) return 7;
            int barrier = open(argv[2], O_RDONLY);
            if (barrier < 0) return 7;
            char armed[128];
            n = snprintf(armed, sizeof armed, "ARM %s\n", label);
            if (emit(armed, (size_t)n)) return 6;
            unsigned char ticket;
            ssize_t got_ticket;
            do { got_ticket = read(barrier, &ticket, 1); } while (got_ticket < 0 && errno == EINTR);
            close(barrier);
            if (got_ticket != 1) return 7;
            memmove(line, line + 2, strlen(line + 2) + 1);
        }
        if (sscanf(line, "P %lu", &seed) == 1) {
            n = snprintf(line, sizeof line, "P %s %lu\n", label, seed);
            if (emit(line, (size_t)n)) return 6;
        } else if (sscanf(line, "B %lu %lu", &count, &seed) == 2) {
            n = snprintf(line, sizeof line, "B %s %lu %lu\n", label, count, seed);
            if (emit(line, (size_t)n)) return 6;
            unsigned char buffer[4096]; /* write chunk size; total bytes come from caller */
            for (unsigned long offset = 0; offset < count;) {
                size_t length = count - offset < sizeof buffer ? count - offset : sizeof buffer;
                for (size_t i = 0; i < length; ++i) buffer[i] = (unsigned char)((offset + i + seed) % 256);
                if (emit(buffer, length)) return 6;
                offset += length;
            }
        } else if (sscanf(line, "E %lu", &count) == 1) {
            /* Binary echo proves the child received every input byte. ISIG is
               restored before the next command, so byte 3 in this payload is
               data while the separate Ctrl+C cases retain terminal semantics. */
            attrs.c_lflag &= (tcflag_t)~ISIG;
            if (tcsetattr(STDIN_FILENO, TCSANOW, &attrs)) return 4;
            n = snprintf(line, sizeof line, "E %s %lu\n", label, count);
            if (emit(line, (size_t)n)) return 6;
            unsigned char buffer[4096];
            while (count) {
                size_t length = count < sizeof buffer ? (size_t)count : sizeof buffer;
                ssize_t got;
                do { got = read(STDIN_FILENO, buffer, length); } while (got < 0 && errno == EINTR);
                if (got <= 0 || emit(buffer, (size_t)got)) return 6;
                count -= (unsigned long)got;
            }
            attrs.c_lflag |= ISIG;
            if (tcsetattr(STDIN_FILENO, TCSANOW, &attrs)) return 4;
            n = snprintf(line, sizeof line, "E_DONE %s\n", label);
            if (emit(line, (size_t)n)) return 6;
        } else if (sscanf(line, "T %lu %lu", &count, &seed) == 2) {
            for (unsigned long i = 0; i < count; ++i) {
                n = snprintf(line, sizeof line, "T %s %lu \033[32m\xe4\xb8\xad" "e\xcc\x81\033[0m\n", label, i);
                if (emit(line, (size_t)n)) return 6;
                struct timespec pause = {(time_t)(seed / 1000000), (long)(seed % 1000000) * 1000};
                while (nanosleep(&pause, &pause) && errno == EINTR) {}
            }
        } else if (!strcmp(line, "V")) {
            /* Cursor saved at a wide right edge, then restored in a narrow view. */
            const char *sample = "\033[2J\033[24;80H\0337\xe4\xb8\xad" "e\xcc\x81";
            if (emit(sample, strlen(sample))) return 6;
        } else if (!strcmp(line, "R")) {
            const char *sample = "\0338\033[0mX\r\n";
            if (emit(sample, strlen(sample))) return 6;
        } else if (sscanf(line, "H %lu", &count) == 1) {
            n = snprintf(line, sizeof line, "H %s\n", label);
            if (emit(line, (size_t)n)) return 6;
            struct timespec pause = {(time_t)count, 0};
            /* Interrupt ends the hold; it deliberately does not restart it. */
            (void)nanosleep(&pause, NULL);
        } else if (!strcmp(line, "Q")) {
            return 0;
        } else {
            const char *bad = "INVALID\n";
            if (emit(bad, strlen(bad))) return 6;
        }
    }
}
