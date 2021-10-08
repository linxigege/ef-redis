package com.wiqer.redis;

import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommandFactory;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Errors;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.RespArray;
import com.wiqer.redis.util.TRACEID;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CommandDecoder extends LengthFieldBasedFrameDecoder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandDecoder.class);
    private static final int MAX_FRAME_LENGTH = Integer.MAX_VALUE;

    public CommandDecoder() {
        super(MAX_FRAME_LENGTH, 0, 4);
    }
    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {

        TRACEID.newTraceId();
        while (in.readableBytes() != 0)
        {
            int mark = in.readerIndex();
            try
            {
                Resp resp = Resp.decode(in);
                if (!(resp instanceof RespArray))
                {
                    throw new IllegalStateException("客户端发送的命令应该只能是Resp Array 类型");
                }
                Command command = CommandFactory.from((RespArray) resp);
                if (command == null)
                {
                    ctx.writeAndFlush(new Errors("unsupport command:" + ((BulkString) ((RespArray) resp).getArray()[0]).getContent().toUtf8String()));
                }
                else
                {
                    return command;
                }
            }
            catch (Exception e)
            {
                in.readerIndex(mark);
                break;
            }
        }
        return null;
    }



}