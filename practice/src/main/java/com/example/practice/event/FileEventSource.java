package com.example.practice.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutionException;

public class FileEventSource implements Runnable{
    private static Logger log = LoggerFactory.getLogger(FileEventSource.class.getName());
    public boolean keepRunning = true;
    private int updateInterval;
    private File file;
    private long filePointer = 0;

    private  EventHandler eventHandler;

    public FileEventSource(int updateInterval, File file, EventHandler eventHandler) {
        this.updateInterval = updateInterval;
        this.file = file;
        this.eventHandler = eventHandler;
    }

    @Override
    public void run() {
        try {
            while (this.keepRunning) {
                Thread.sleep(this.updateInterval);
                // file 크기를 계산
                // 계산하는 이유는 파일의 내용이 증가되거나 감소되는지 체크
                long len = this.file.length();
                //
                if(len < this.filePointer) {
                    log.debug("file was reset as filePointer is longer than file length");
                    filePointer = len;
                } else if(len > this.filePointer) {
                    readAppendAndSend();
                } else {
                    continue;
                }
            }
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        } catch (ExecutionException e) {
            log.error(e.getMessage());
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void readAppendAndSend() throws IOException, ExecutionException, InterruptedException {
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        raf.seek(this.filePointer);
        String line = null;
        while ((line = raf.readLine()) != null) {
            sendMessage(line);
        }
        // file이 변경되었으므로 file의 filePointer를 현재 file의 마지막으로 제설정
        this.filePointer = raf.getFilePointer();
    }

    private void sendMessage(String line) throws ExecutionException, InterruptedException {
        String[] tokens = line.split(",");
        String key = tokens[0];
        StringBuffer value = new StringBuffer();

        for(int i = 1; i < tokens.length; i++) {
            if(i != (tokens.length - 1)) {
                value.append(tokens[i] + ",");
            } else {
                value.append(tokens[i]);
            }
        }
        MessageEvent messageEvent = new MessageEvent(key, value.toString());
        this.eventHandler.onMessage(messageEvent);
    }
}
