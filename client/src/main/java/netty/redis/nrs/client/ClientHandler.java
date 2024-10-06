package netty.redis.nrs.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;

import java.util.Scanner;

public class ClientHandler extends ChannelInboundHandlerAdapter{
    private Scanner scanner;
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        scanner = new Scanner(System.in);
        new Thread(() -> {
            while (true){
                String message = scanner.nextLine();
                ctx.writeAndFlush(Unpooled.copiedBuffer(message, CharsetUtil.UTF_8));
            }
        }).start();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        //接收服务端发送过来的消息
        ByteBuf byteBuf = (ByteBuf) msg;
        System.out.println("返回的内容为：" + byteBuf.toString(CharsetUtil.UTF_8));
    }
}
