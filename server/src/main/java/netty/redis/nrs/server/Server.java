package netty.redis.nrs.server;

import cn.hutool.core.io.FileUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import netty.redis.nrs.server.service.CommandService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Server {
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final CommandService commandService = CommandService.getInstance();
    static Integer initialDelay;
    static Integer period;
    static TimeUnit timeUnit;

    public static void main(String[] args) throws Exception{
        CommandLine commandLine = createCommandLine(args);
        Map<String, Object> setting = config(commandLine.getOptionValue("c"));
        readConfig(setting);
        commandService.init(setting);
        scheduler.scheduleAtFixedRate(commandService::clean,initialDelay,period, timeUnit);
        connection();
    }
    public static void connection() throws Exception{
        //创建两个线程组 boosGroup、workerGroup
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            //创建服务端的启动对象，设置参数
            ServerBootstrap bootstrap = new ServerBootstrap();
            //设置两个线程组boosGroup和workerGroup
            bootstrap.group(bossGroup, workerGroup)
                    //设置服务端通道实现类型
                    .channel(NioServerSocketChannel.class)
                    //设置线程队列得到连接个数
                    .option(ChannelOption.SO_BACKLOG, 128)
                    //设置保持活动连接状态
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    //使用匿名内部类的形式初始化通道对象
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            //给pipeline管道设置处理器
                            socketChannel.pipeline().addLast(new ServerHandler());
                        }
                    });//给workerGroup的EventLoop对应的管道设置处理器
            //绑定端口号，启动服务端
            ChannelFuture channelFuture = bootstrap.bind(6666).sync();
            //对关闭通道进行监听
            channelFuture.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private static CommandLine createCommandLine(String... args) throws Exception {
        Options options = new Options();
        options.addOption("h","help",false,"print help");
        options.addOption("c","configPath",true,"the path of config");
        CommandLineParser parser = new DefaultParser();
        try{
            return parser.parse(options, args);
        }catch (Exception e){
            throw new Exception(e.getMessage());
        }
    }

    private static Map<String, Object> config(String configPath) throws Exception {
        File[] configFiles = FileUtil.ls(configPath);
        File file = configFiles[0];
        InputStream inputStream = new FileInputStream(file);
        Yaml yaml = new Yaml();
        return yaml.load(inputStream);
    }

    private static void readConfig(Map<String,Object> setting){
        Iterator<String> iterator = setting.keySet().iterator();
        iterator.next();
        String timerKey = iterator.next();
        Map<String,Object> config = (Map<String,Object>) setting.get(timerKey);
        initialDelay = (Integer) config.get("initialDelay");
        period = (Integer) config.get("period");
        String unit = (String) config.get("unit");
        timeUnit = TimeUnit.valueOf(unit);
    }
}
