package netty.redis.nrs.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.util.CharsetUtil;
import netty.redis.nrs.server.service.Command;
import netty.redis.nrs.server.service.CommandService;


public class ServerHandler extends ChannelInboundHandlerAdapter {

    private static final CommandService commandService = CommandService.getInstance();
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ProcessMessage(ctx,(ByteBuf) msg);
    }

    private void ProcessMessage(ChannelHandlerContext ctx,ByteBuf msg) {
        Object result;
        //获取客户端发送过来的消息
        String message = msg.toString(CharsetUtil.UTF_8);

        System.out.println("收到客户端的消息："+message);

        String[] strings = message.split(" ");
        String name = strings.length > 0 ? strings[0] : null;
        String key = strings.length > 1 ? strings[1] : null;
        String value = strings.length > 2 ? strings[2] : null;

        Command command = new Command(name,key,value);
        result = commandService.executeCommand(command);

        ctx.writeAndFlush(Unpooled.copiedBuffer(result.toString(), CharsetUtil.UTF_8));
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        //发生异常，关闭通道
        ctx.close();
    }
}
