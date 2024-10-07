package netty.redis.nrs.server.service;

import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import netty.redis.nrs.server.storage.MemoryStorage;
import netty.redis.nrs.server.storage.Record;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;

public class CommandService {
    private static final CommandService commandService = new CommandService();
    private CommandService(){
        initializeCommands();
    }
    public static CommandService getInstance(){
        return commandService;
    }

    private static final Integer TYPE_INTEGER = 0;
    private static final Integer TYPE_STRING = 1;
    private static final MemoryStorage memoryStorage = MemoryStorage.getInstance();
    private final Map<String, Function<Command, String>> commandMap = new HashMap<>();

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

    public String executeCommand(Command command) {
        String result;
        Function<Command, String> action = commandMap.get(command.getName());
        if (action != null) {
            result = action.apply(command);
        } else {
            result = "不存在的指令: " + command.getName();
        }
        return result;
    }

    public void init(Map<String,Object> setting) throws Exception{
        memoryStorage.initMap(setting);
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
        Record record = memoryStorage.get(command.getKey());
        if(record.getType() == null){
            return "0";
        }
        return "1";
    }

    public String setNX(Command command){
        if(command.getKey() != null){
            Record record = memoryStorage.get(command.getKey());
            if(record.getType() != null){
                return "0";
            }
            memoryStorage.set(memoryStorage.buffer,memoryStorage.map,command.getKey(), 1);
            return "1";
        }else {
            return "-1";
        }
    }

    public String incr(Command command){
        return memoryStorage.incr(command.getKey());
    }

    public String decr(Command command){
        return memoryStorage.decr(command.getKey());
    }

    public String setKey(Command command){
        if(command.getKey() != null && command.getValue() != null){
            String value = command.getValue();
            try{
                int newValue = Integer.parseInt(value);
                memoryStorage.set(memoryStorage.buffer,memoryStorage.map,command.getKey(), newValue);
            }catch (Exception e){
                memoryStorage.set(memoryStorage.buffer,memoryStorage.map,command.getKey(), command.getValue());
            }
            return "1";
        }else {
            return "0";
        }
    }

    public String getValue(Command command){
        String result;
        Record record = memoryStorage.get(command.getKey());
        if(record.getType() != null){
            if("Integer".equals(record.getType())){
                ByteBuffer byteBuffer = ByteBuffer.wrap(record.getValue());
                result = Integer.toString(byteBuffer.getInt());
            }else if("String".equals(record.getType())){
                result = new String(record.getValue(), CharsetUtil.UTF_8);
            }else {
                List<Object> list = getList(record);
                result = list.toString();
            }
        }else {
            result = "null";
        }
        return result;
    }

    private List<Object> getList(Record record) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(record.getValue());
        int size = byteBuffer.getInt();
        List<Object> list = new ArrayList<>();
        for(int i = 0;i < size;i++){
            int type = byteBuffer.getInt();
            if(type == TYPE_STRING){
                int length = byteBuffer.getInt();
                byte[] bytes = new byte[length];
                byteBuffer.get(bytes);
                list.add(new String(bytes,CharsetUtil.UTF_8));
            }else {
                list.add(byteBuffer.getInt());
            }
        }
        return list;
    }

    public String stat(){
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

    public void clean(){
        memoryStorage.clean();
    }
}
