CC := gcc
CFLAGS := -std=c11 -Wall -Wextra -Wpedantic -pthread
TARGET := packet-router
SOURCE := src/roteador.c

.PHONY: all run debug tsan clean

all: $(TARGET)

$(TARGET): $(SOURCE)
	$(CC) $(CFLAGS) $(SOURCE) -o $(TARGET)

run: $(TARGET)
	./$(TARGET)

debug:
	$(CC) $(CFLAGS) -g -O0 $(SOURCE) -o $(TARGET)-debug

tsan:
	$(CC) $(CFLAGS) -g -O1 -fsanitize=thread $(SOURCE) -o $(TARGET)-tsan
	./$(TARGET)-tsan

clean:
	rm -f $(TARGET) $(TARGET)-debug $(TARGET)-tsan
