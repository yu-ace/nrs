package netty.redis.nrs.server.service;

import netty.redis.nrs.server.storage.MemoryStorage;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

public class CommandService {
    private static final CommandService commandService = new CommandService();
    private CommandService(){
        initializeCommands();
    }
    public static CommandService getInstance(){
        return commandService;
    }
    private static final MemoryStorage memoryStorage = MemoryStorage.getInstance();
    private final Map<String, Function<Command, Object>> commandMap = new HashMap<>();

    public void initializeCommands() {
        commandMap.put("set", this::setKey);
        commandMap.put("get", this::getValue);
        commandMap.put("setNX", this::setNX);
        commandMap.put("exists",this::exists);
        commandMap.put("stat", command -> stat());
        commandMap.put("delete", this::deleteKey);
        commandMap.put("list", command -> list());
        commandMap.put("decr",this::decr);
        commandMap.put("incr",this::incr);
        commandMap.put("shutDown",command -> shutDown());
    }

    public Object executeCommand(Command command) {
        Object result;
        Function<Command, Object> action = commandMap.get(command.getName());
        if (action != null) {
            result = action.apply(command);
        } else {
            result = "不存在的指令: " + command.getName();
        }
        return result;
    }

    public void init() throws Exception{
        memoryStorage.initMap();
        memoryStorage.init();
    }

    public String shutDown() {
        try{
            memoryStorage.shutDown();
            memoryStorage.saveMap();
        }catch (Exception e){
            e.getStackTrace();
        }
        return "save ok";
    }

    public String exists(Command command){
        Object s = memoryStorage.get(command.getKey());
        if(s != " " && s != "null"){
            return "key 存在";
        }
        return "key 不存在";
    }
    public String setNX(Command command){
        if(command.getKey() != null){
            Object s = memoryStorage.get(command.getKey());
            if(!(Objects.equals(s, " ")) && !(Objects.equals(s, "null"))){
                return "key 存在";
            }
            memoryStorage.set(memoryStorage.buffer,memoryStorage.map,command.getKey(), 1);
            return "key 添加成功";
        }else {
            return "error";
        }
    }

    public Object incr(Command command){
        return memoryStorage.incr(command.getKey());
    }

    public Object decr(Command command){
        return memoryStorage.decr(command.getKey());
    }

    public String setKey(Command command){
        if(command.getKey() != null && command.getValue() != null){
            memoryStorage.set(memoryStorage.buffer,memoryStorage.map,command.getKey(), command.getValue());
            return "set ok";
        }else {
            return "set error";
        }
    }

    public Object getValue(Command command){
        return memoryStorage.get(command.getKey());
    }

    public Object stat(){
        Integer usedMemory = memoryStorage.usedMemory();
        int freeMemory = memoryStorage.FreeMemory();
        return "usedMemory:"+ usedMemory+"\nfreeMemory:" + freeMemory;
    }

    public String deleteKey(Command command){
        return memoryStorage.delete(command.getKey());
    }

    public String list() {
        StringBuilder stringBuilder = new StringBuilder();
        Set<String> keys = memoryStorage.keyList();
        if(keys.size() != 0){
            for(String key:keys){
                stringBuilder.append(key).append(" ");
            }
            return stringBuilder.toString();
        }else {
            return "null";
        }
    }

    public void clean() {
        synchronized (this){
            ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 1024);
            Set<String> strings = memoryStorage.keyList();
            for(String s:strings){
                memoryStorage.set(byteBuffer,memoryStorage.map,s,memoryStorage.get(s));
            }
            memoryStorage.buffer = byteBuffer;
        }
    }
}
