package com.wiqer.redis.aof;


import com.wiqer.redis.RedisCore;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.util.Format;
import com.wiqer.redis.util.PropertiesUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import jdk.nashorn.internal.ir.WhileNode;
import org.jetbrains.annotations.NotNull;

import javax.swing.text.Segment;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * @author lilan
 */
public class Aof2 {

    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(Aof2.class);

    private static final String suffix = ".aof";

    public static final int shiftBit = 26;
    private Long aofPutIndex = 0L;
    private String fileName = PropertiesUtil.getAofPath();
    private RingBlockingQueue<Resp> runtimeRespQueue = new RingBlockingQueue<>(8888, 888888);
    ByteBuf bufferPolled = new PooledByteBufAllocator().buffer(8888, 2147483647);

    private ScheduledThreadPoolExecutor persistenceExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {

        @Override
        public Thread newThread(@NotNull Runnable r) {
            return new Thread(r, "Aof_Single_Thread");
        }
    });

    private final RedisCore redisCore;

    /**
     * 读写锁
     */
    final ReadWriteLock reentrantLock = new ReentrantReadWriteLock();

    public Aof2(RedisCore redisCore) {
        this.redisCore = redisCore;
        File file = new File(this.fileName + suffix);
        if (!file.isDirectory()) {
            File parentFile = file.getParentFile();
            if (null != parentFile && !parentFile.exists()) {
                parentFile.mkdir();///创建文件夹
            }
        }
        start();
    }

    public void put(Resp resp) {
        runtimeRespQueue.offer(resp);
    }

    public void start() {
        /**
         * 谁先执行需要顺序异步执行
         */
        persistenceExecutor.execute(() -> pickupDiskDataAllSegment());
        persistenceExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                downDiskAllSegment();
            }
        }, 10, 1, TimeUnit.SECONDS);
    }

    public void close() {
        try {
            persistenceExecutor.shutdown();
        } catch (Exception e) {
            LOGGER.warn("Exception!", e);
        }
    }

    public void downDiskAllSegment(){
        if (reentrantLock.writeLock()){
            try {
                long segmentId = -1;
                long segmentGroupHead = -1;
                /**
                 * you hua nei cun
                 */
                Segment:
                while (segmentId!=(aofPutIndex>>shiftBit)){
                    //要后28位
                    segmentId = (aofPutIndex>>shiftBit);
                    RandomAccessFile randomAccessFile = new RandomAccessFile(fileName + "-" + segmentId + suffix, "rw");
                    FileChannel channel = randomAccessFile.getChannel();
                    long len = channel.size();
                    Format.uintNBit()

                }
            }
        }
    }


}
